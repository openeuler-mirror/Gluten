# cpp-omni

## Allocation listener boundary

Omni owns `omniruntime::mem::PoolListener` in `memory/pool_listener.h`.
Gluten's `AllocationListener` remains a separate type.
`OmniPoolAllocationListener` adapts between them using both real headers;
never cast a Gluten listener pointer to an Omni listener pointer or redeclare
one engine's class in the other engine.

Task managers and broadcast entries keep the adapter and its delegate alive
until their pool has finished teardown. Existing reservation-denial exceptions
pass through the adapter unchanged. Rebuild and deploy Omni and this backend
together when moving from the former duplicated-listener ABI.
