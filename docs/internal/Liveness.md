# Liveness

Listeners must be completed eventually.
Resources (memory buffers, file handles, etc) must be released eventually.
Rely on the tools, frameworks and infrastructure within Elasticsearch to ensure liveness wherever possible.
Use comments to explain situations whose liveness properties are not clear.

## Local reasoning

Write code so that its liveness properties are clear to the reader.
Code which calls asynchronous methods may assume that the called method will eventually complete the provided listener, unless its documentation indicates otherwise.
Code which acquires a resource should make it clear to the reader how that resource is released. This often means either closing the resource in a `finally` block, or attaching the release of the resource to the completion of a listener, or
transferring the ownership of the resource into another resource whose eventual release is guaranteed.

## Principles

Methods which do not work with `ActionListener` (and do not block a thread) have no liveness issues.

Asynchronous methods which accept an `ActionListener` must on every path either complete the listener directly or pass the listener on to another asynchronous method (whose liveness properties may be assumed by the caller). Such methods may instead throw an exception, if they are only ever called in a situation where such exceptions are caught and passed to all waiting listeners without leaking any attached resources.

Methods which create a `GroupedActionListener` must ensure that it is eventually completed the correct number of times.

Methods which create a `RefCountedListener` must ensure that it is eventually closed, and that every acquired listener is eventually completed.

Methods which create a `RefCountedRunnable` must ensure that it is eventually closed, and that every acquired reference is eventually closed.

Assume that `AbstractRunnable` tasks submitted to a threadpool eventually execute.

Assume that tasks submitted for execution by the `MasterService` eventually complete.

Assume that actions invoked on a different node via the `TransportService` eventually complete.
