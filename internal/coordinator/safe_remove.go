package coordinator

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"vault/internal/events"
	"vault/internal/integrity"
	"vault/internal/metadata"
	"vault/internal/storage"
)

// ErrRemovalRefused means the guarded removal path would not delete a
// replica: doing so could destroy the only verified copy, or a listed
// replica that no committed replacement has taken over yet.
var ErrRemovalRefused = errors.New("replica removal refused")

// Classes of replica removal, decided against the latest record.
const (
	// RemovalDeletedObject: the latest record is a tombstone. The object's
	// data must never come back; there is no valid copy to preserve.
	RemovalDeletedObject = "deleted_object"
	// RemovalStale: a version older than the current live version.
	RemovalStale = "stale"
	// RemovalUncommitted: a version newer than every committed record (a
	// write that failed and whose rollback could not reach the node).
	RemovalUncommitted = "uncommitted"
	// RemovalInvalid: an unlisted copy of the current version whose re-hash
	// or version does not match metadata.
	RemovalInvalid = "invalid_copy"
	// RemovalExtra: an unlisted, verified copy of the current version while
	// the listed replicas already meet RF.
	RemovalExtra = "extra_copy"
)

// RemovalResult reports one guarded removal.
type RemovalResult struct {
	ObjectID       string `json:"object_id"`
	NodeID         string `json:"node_id"`
	Version        uint64 `json:"version"`
	CurrentVersion uint64 `json:"current_version"`
	Class          string `json:"class"`
	Reason         string `json:"reason"`
	// Need is how many verified copies of the current version, on other
	// nodes, the removal required; VerifiedBy lists the ones found.
	Need          int      `json:"need"`
	VerifiedBy    []string `json:"verified_by,omitempty"`
	Removed       bool     `json:"removed"`
	AlreadyAbsent bool     `json:"already_absent,omitempty"`
	Refused       string   `json:"refused,omitempty"`
}

// removalProof is evidence that nodes hold a verified copy (version and
// checksum) of version, gathered by the caller in the same operation.
type removalProof struct {
	version uint64
	nodes   []string
}

// removeReplicaLocked is the single guarded path by which Vault destroys a
// replica that could still be a valid copy of some version. The order is
// fixed: classify against the latest record → verify (the copy itself where
// its validity matters, and enough OTHER copies of the current version:
// version AND checksum) → only then delete that one version.
//
//   - a listed replica of the current version is never removed here: it
//     must first be replaced by a verified copy through a committed
//     metadata change (repair, reconciliation, rebalance), after which it
//     is no longer listed;
//   - an extra valid copy needs RF verified listed copies elsewhere;
//   - stale, uncommitted or invalid bytes need one verified copy of the
//     current version elsewhere — so the last physical remnant of an
//     object whose current version cannot be verified is kept;
//   - data of a deleted object needs none: the tombstone, re-read here, is
//     the authority, and the data must never resurrect.
//
// The caller holds the object lock (c.locks) and passes rec, the latest
// record read under it. Removing an unlisted copy of the current version
// also requires the caller to hold the object's repair guard, so no repair
// can be writing that very copy.
func (c *Coordinator) removeReplicaLocked(ctx context.Context, rec metadata.Object, nodeID string, version uint64, reason string, proof *removalProof) (RemovalResult, error) {
	res := RemovalResult{ObjectID: rec.ID, NodeID: nodeID, Version: version, CurrentVersion: rec.Version, Reason: reason}
	listed := !rec.Deleted && slices.Contains(rec.Replicas, nodeID)
	switch {
	case rec.Deleted:
		res.Class = RemovalDeletedObject
	case version < rec.Version:
		res.Class, res.Need = RemovalStale, 1
	case version > rec.Version:
		res.Class, res.Need = RemovalUncommitted, 1
	case listed:
		res.Class = "listed_current"
		return c.refuse(ctx, res, "a listed replica of the current version is removed only after a verified replacement is committed")
	default:
		ok, err := c.verifyCopy(ctx, rec, nodeID)
		if errors.Is(err, storage.ErrNotFound) {
			res.AlreadyAbsent = true
			return res, nil
		}
		if err != nil && !errors.Is(err, errCopyInvalid) {
			return c.refuse(ctx, res, "cannot verify the copy itself: "+err.Error())
		}
		if ok {
			res.Class, res.Need = RemovalExtra, rec.ReplicationFactor
		} else {
			res.Class, res.Need = RemovalInvalid, 1
		}
	}
	if res.Need > 0 {
		res.VerifiedBy = c.proveCopies(ctx, rec, nodeID, res.Need, proof)
		if len(res.VerifiedBy) < res.Need {
			return c.refuse(ctx, res, fmt.Sprintf("only %d of %d required verified copies of v%d exist on other nodes",
				len(res.VerifiedBy), res.Need, rec.Version))
		}
	}

	if hook := c.hooks.beforeReplicaDelete; hook != nil {
		hook(rec.ID, nodeID, version)
	}
	n, err := c.node(nodeID)
	if err == nil {
		err = n.DeleteReplica(ctx, rec.ID, version)
	}
	switch {
	case errors.Is(err, storage.ErrNotFound):
		res.AlreadyAbsent = true
		return res, nil
	case err != nil:
		c.reportIfUnavailable(ctx, nodeID, err)
		return res, fmt.Errorf("remove %s v%d from %s: %w", rec.ID, version, nodeID, err)
	}
	res.Removed = true
	c.noteRemoval(res.Class, true)
	c.log.Emit(ctx, events.ReplicaRemoved, "object_id", rec.ID, "node_id", nodeID, "version", version,
		"current_version", rec.Version, "class", res.Class, "reason", reason,
		"need", res.Need, "verified_by", res.VerifiedBy, "object_deleted", rec.Deleted)
	return res, nil
}

func (c *Coordinator) refuse(ctx context.Context, res RemovalResult, why string) (RemovalResult, error) {
	res.Refused = why
	c.noteRemoval(res.Class, false)
	c.log.Warn(ctx, events.ReplicaRemovalRefused, "object_id", res.ObjectID, "node_id", res.NodeID,
		"version", res.Version, "current_version", res.CurrentVersion, "class", res.Class,
		"reason", res.Reason, "need", res.Need, "verified_by", res.VerifiedBy, "why", why)
	return res, fmt.Errorf("%w: %s v%d on %s: %s", ErrRemovalRefused, res.ObjectID, res.Version, res.NodeID, why)
}

var errCopyInvalid = errors.New("copy does not match metadata")

// verifyCopy asks nodeID to re-hash its copy of rec's current version and
// checks both the version its record names and the checksum against
// metadata. It returns (true, nil) only for a verified copy.
func (c *Coordinator) verifyCopy(ctx context.Context, rec metadata.Object, nodeID string) (bool, error) {
	n, err := c.node(nodeID)
	if err != nil {
		return false, err
	}
	vctx, cancel := context.WithTimeout(ctx, c.cfg.ReplicaTimeout)
	vr, err := n.VerifyReplica(vctx, rec.ID, rec.Version)
	cancel()
	if err != nil {
		c.reportIfUnavailable(ctx, nodeID, err)
		return false, err
	}
	if vr.ObjectID != rec.ID || vr.Version != rec.Version {
		return false, fmt.Errorf("%w: node record names %s v%d", errCopyInvalid, vr.ObjectID, vr.Version)
	}
	if err := integrity.Verify(integrity.Digest{SHA256: rec.SHA256, Size: rec.Size}, vr.Actual); err != nil {
		return false, fmt.Errorf("%w: %v", errCopyInvalid, err)
	}
	return true, nil
}

// proveCopies returns up to need nodes, other than exclude, that are listed
// for rec's current version and hold a verified copy of it. Evidence in
// proof counts only if it is for rec's current version and names listed
// nodes; everything else is verified now.
func (c *Coordinator) proveCopies(ctx context.Context, rec metadata.Object, exclude string, need int, proof *removalProof) []string {
	var out []string
	if rec.Deleted {
		return nil
	}
	if proof != nil && proof.version == rec.Version {
		for _, n := range proof.nodes {
			if n != exclude && slices.Contains(rec.Replicas, n) && !slices.Contains(out, n) {
				out = append(out, n)
			}
		}
	}
	for _, n := range c.readOrder(rec, "") {
		if len(out) >= need {
			break
		}
		if n == exclude || slices.Contains(out, n) || c.health.State(n) != storage.StateHealthy {
			continue
		}
		if ok, _ := c.verifyCopy(ctx, rec, n); ok {
			out = append(out, n)
		}
	}
	if len(out) > need {
		out = out[:need]
	}
	return out
}
