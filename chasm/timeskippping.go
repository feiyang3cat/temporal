package chasm

// TimeSkippingRoot is an optional capability of a root component that opts into time skipping (by
// calling MutableContext.InitTimeSkippingConfig). The CHASM framework calls HasInflightWork during
// CloseTransaction to decide whether the execution is idle enough to fast-forward virtual time.
//
// Time skipping only advances virtual time while the execution is idle. The framework already knows
// the execution's pending timer tasks (the skip target is the earliest future-scheduled one), but it
// cannot tell, purely structurally, the difference between "waiting out a timer/backoff" (idle, safe
// to skip) and "work dispatched/running" (in flight, must not skip past). Only the root component
// knows its own semantics, so it reports in-flight work here.
//
// HasInflightWork must return true whenever advancing virtual time would skip past real work — e.g.
// a standalone activity whose attempt has been dispatched to a worker or is currently running. It
// returns false only when the sole thing the execution is waiting on is a future timer (start delay,
// retry backoff, scheduled wake), which is exactly what time skipping exists to fast-forward.
type TimeSkippingRoot interface {
	HasInflightWork(ctx Context) bool
}
