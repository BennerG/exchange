package consumer

import "context"

// ReconcileOnceForTest exposes reconcileOnce for testing. This file
// compiles only as part of the test binary, never into the real one.
func (r *Reconciler) ReconcileOnceForTest(ctx context.Context) {
	r.reconcileOnce(ctx)
}
