# Liveness

Listeners must be completed eventually.
Resources (file handles, reference-count increments, circuit-breaker reservations, pooled memory allocations, etc) must be released eventually.
Write code so that its liveness properties are clear to the reader.
Rely on the tools, frameworks and infrastructure within Elasticsearch to ensure liveness wherever possible.
Avoid liveness conditions which require complex reasoning across many pages of code as they often harbor (present or future) bugs.
If complex reasoning is unavoidable, document the liveness argument within the code.

## Local reasoning

Code which calls an asynchronous method may usually assume that the called method will either eventually complete the provided listener, or it throws an exception.

Code which acquires a resource should make it clear to the reader how that resource is released.
This often means one of:
* closing the resource in a `finally` block,
* attaching the release of the resource to the completion of a listener using `ActionListener.releasing` or `ActionListener.releaseBefore` or `ActionListener.releaseAfter` or similar
* transferring the ownership of the resource into another resource whose eventual release is guaranteed.

### Node shutdown

For the sake of simplicity, ignore liveness issues that only occur when the node shuts down.

## Principles

Methods which do not work with `ActionListener` or equivalent, do not acquire a resource, and do not block a thread, have no liveness issues. Ignore blocking on disk I/O when considering liveness.

Asynchronous methods which accept an `ActionListener` must on every path either complete the listener directly or pass the listener on to another asynchronous method.
Many such methods may throw an exception instead of completing the listener.
If the method also acquires resources, it must either ensure that those resources are released on exception or else justify why no exception is possible.

Methods which create a `GroupedActionListener` must ensure that it is eventually completed the correct number of times.

Methods which create a `RefCountingRunnable` must ensure that it is eventually closed, and that every acquired listener is eventually completed.

Methods which create a `RefCountingRunnable` must ensure that it is eventually closed, and that every acquired reference is eventually closed.

Subsystems which accumulate a collection (e.g. a queue) of listeners, or of resources, must ensure that the collection is eventually drained.

A common pattern is for an asynchronous method to partially modify the behavior of its listener using utilities such as `ActionListener#delegateFailure` and `ActionListener#delegateResponse`, passing
the modified listener on to other asynchronous code. The modifying callback must complete its listener argument on all paths.

## Reasoning heuristics

Assume that utilities which decorate an `ActionListener` with extra behavior (such as the static methods on `ActionListener` itself) generally complete the decorated listener.

Assume that `AbstractRunnable` tasks submitted to a threadpool eventually execute.

Assume that tasks submitted for execution by the `MasterService` eventually complete.

Assume that actions invoked via the `TransportService` eventually complete.

