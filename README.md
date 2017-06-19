This is a specialized queuing construct that is intended for a two-phase
scenario where in the first-phase events need to be streamed to disk until the
consumer is ready for those events. After readiness is reached, the events are
delivered to the consumer in the original order by draining the slow-start
buffer and then seamless switching to pass-through of ongoing produced events.

![](SlowStartEventQueues.png)
