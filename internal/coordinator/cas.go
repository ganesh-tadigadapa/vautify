package coordinator

import (
	"context"
	"errors"
	"fmt"

	"vault/internal/events"
	"vault/internal/metadata"
)

// ErrWriteConflict is wrapped by every *ConflictError.
var ErrWriteConflict = errors.New("write conflict")

// ConflictError reports a failed compare-and-swap: the caller's expected
// version is not the object's current live version. Nothing was changed.
type ConflictError struct {
	ObjectID        string `json:"object_id"`
	Operation       string `json:"operation"`
	ExpectedVersion uint64 `json:"expected_version"`
	// CurrentVersion is the current live version, or 0 if there is none.
	CurrentVersion uint64 `json:"current_version"`
	// CurrentState is "live", "deleted" (tombstone) or "absent".
	CurrentState string `json:"current_state"`
	// TombstoneVersion is set when CurrentState is "deleted".
	TombstoneVersion uint64 `json:"tombstone_version,omitempty"`
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("write conflict on %s: expected version %d, current is %d (%s)",
		e.ObjectID, e.ExpectedVersion, e.CurrentVersion, e.CurrentState)
}

func (e *ConflictError) Unwrap() error { return ErrWriteConflict }

// checkExpected enforces CAS semantics. expected == nil means no condition.
// expected == 0 means "no live object may exist" (create-only); otherwise
// the current live version must equal *expected.
func (c *Coordinator) checkExpected(ctx context.Context, id string, expected *uint64, op string) error {
	if expected == nil {
		return nil
	}
	rec, err := c.meta.GetRecord(ctx, id)
	if err != nil && !errors.Is(err, metadata.ErrNotFound) {
		return err
	}
	ce := &ConflictError{ObjectID: id, Operation: op, ExpectedVersion: *expected, CurrentState: "absent"}
	switch {
	case err != nil:
	case rec.Deleted:
		ce.CurrentState, ce.TombstoneVersion = "deleted", rec.Version
	default:
		ce.CurrentState, ce.CurrentVersion = "live", rec.Version
	}
	if ce.CurrentVersion == *expected {
		return nil
	}
	c.log.Warn(ctx, events.WriteConflict, "object_id", id, "operation", op,
		"expected_version", *expected, "current_version", ce.CurrentVersion, "current_state", ce.CurrentState)
	return ce
}
