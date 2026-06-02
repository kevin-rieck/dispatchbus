# Repository Roadmap
updated: 2026-05-26

## Goal
Define a conservative, dependency-aware roadmap for `dispatchr` that makes sequencing constraints explicit, highlights which work can proceed in parallel, and keeps production-oriented reliability features subordinate to core correctness.

## Audience
The roadmap is optimized for small backend teams that want a dependable in-process message bus for service-layer applications without adopting a full framework.

## Roadmap posture
This roadmap is intentionally conservative. It communicates direction and dependency order, not a dense release-by-release promise.

## Guiding principles
- Correctness before expansion
- Small-team ergonomics over framework breadth
- Primitives over heavy policy
- Optional production features that do not bloat the core
- Clear non-goals to preserve focus

## Dependency-based roadmap lanes

### Lane 1: Foundation semantic hardening
This is the dependency root for the near-term roadmap. The main goal is to make dispatch behavior trustworthy under concurrency, failure, and shutdown.

Primary outcomes:
- tighten dispatch behavior under handler failure
- strengthen nested dispatch and event publication guarantees
- sharpen shutdown and close semantics
- increase confidence through stronger concurrency, failure-path, and type coverage

Parallel workstreams inside this lane:
- failure handling and publication semantics
- shutdown and close behavior
- verification work that exercises current semantics without committing new product surface area

### Lane 2: Reliability primitives and backend correctness
This lane can begin once foundation semantics are mostly clear. It should preserve backend abstraction, but only through narrow seams that are justified by current reliability work.

Primary outcomes:
- preserve protocol-driven seams for storage and publishing behavior
- complete multi-worker-safe outbox behavior for SQLite
- mature the SQLite outbox implementation
- improve recovery-oriented guidance for at-least-once delivery once implementation behavior settles

Parallel workstreams inside this lane:
- minimal storage and publishing seam design
- SQLite-specific outbox correctness and multi-worker behavior

Constraint:
- backend abstraction is a design constraint, not a standalone feature program; broad future-backend work should not block SQLite correctness

### Lane 3: Public contract stabilization
This lane starts with discovery work that can run in parallel, then converges later once semantic behavior and reliability shape have settled.

Primary outcomes:
- define the stable public API clearly
- distinguish core API from optional helpers and adapters
- make extension points intentional rather than accidental
- clarify guarantees and non-guarantees after the upstream semantics are stable

Parallel workstreams inside this lane:
- early API and export inventory
- early extension-point audit

Later convergence work:
- final public API commitments
- final guarantee and non-guarantee language

### Lane 4: Production adoption guidance
This lane is mostly downstream of the first three lanes. It should document stable recommendations rather than chase moving targets.

Primary outcomes:
- operational guidance for production use
- examples for service-layer backend applications
- explicit best-fit and poor-fit guidance
- documentation for delivery semantics, trade-offs, and extension patterns

## Near-term parallelization summary
The safest near-term workstreams to run in parallel are:
- handler failure semantics
- shutdown and close semantics
- stronger tests and type coverage
- API surface audit
- narrow storage and publishing seam design
- SQLite outbox correctness work that stays within those narrow seams

## Downstream convergence items
These items should mostly wait for upstream semantic and reliability convergence:
- final stable public API commitments
- retry extension hooks
- idempotency extension hooks
- final guarantees and non-guarantees documentation
- most production-facing guidance

## Non-goals for the near term
The roadmap should explicitly avoid early expansion into heavy policy features, including:
- dead-letter queue management
- saga or workflow-engine behavior
- complex retry policy matrices
- broad enterprise orchestration features

It should also avoid turning backend abstraction into a broad integration program before the core semantics and current reliability primitives have settled.

## Success criteria
The roadmap is successful if it helps the repository:
- become more trustworthy under real failure conditions
- make true dependencies and parallel work visible to contributors
- present a smaller, clearer API surface to adopters
- support practical reliability patterns for small backend teams
- stay lightweight while still being production-credible

## Recommended public framing
If published in the repository, the roadmap should be presented as a set of dependency-aware lanes rather than dated commitments or rigid phases. The key message should be:

1. harden the semantics
2. mature reliability primitives through narrow seams
3. stabilize the public contract after convergence
4. document safe adoption patterns once the shape is stable

## Scope
This roadmap does not attempt to plan exact versions, dates, or a broad ecosystem strategy. It is a prioritization document for the next stage of the repository's evolution.
