# intracom
[![codecov](https://codecov.io/gh/ambitiousfew/intracom/branch/main/graph/badge.svg?token=EI0ZYE5ZIZ)](https://codecov.io/gh/ambitiousfew/intracom)

> NOTE: Intracom went through a big refactor from using mutex locks to not using locks. It should still be used with caution and treated as an alpha project.
> A few basic examples have been provided in the `examples/` folder 

Small pub/sub library to perform intra-communication within a go program and its routines. It is a wrapper around go channels and uses no mutex locks.


## Under the hood
Intracom starts its own routines to handle Subscribe, unsubscribe (closure), Register, and unregister (closure) calls as if they were requests.
They go down a "requests" channel where a routine in charge of receiving those requests manages its own local state of the publisher/subscriber channels.

Any new registered topic is given its own routine to handle new subscribe/unsubscribe requests as well as publishing. Only NEW subscribes and first-time unsubscribes will interrupt the "broadcaster" doing the publishing **only within this specific topic**. This means you can register many topics and slow publishers and/or slow subscribers do not impede the broadcasts being done across other topics.

Though a drawback of this can be the subscribers within the same topic CAN impede each other from receiving messages usually due to the buffer filling up and backpressuring against the publisher. But this can be somewhat controlled by using different buffer policies. A `BufferPolicy` is effectively you choosing how the broadcaster of the topic will react to a given subscriber when it reaches a full buffer for **that** specific subscribers channel. Using DropNone or a Drop with timeout policy on any consumer of a topic can cause the next broadcast to delay. So keep this in mind when choosing a buffer policy.

## Things to Know + Gotchas

### Closing early
Calling `.Close()` on the intracom instance should **ALWAYS** be the last thing you do, it is your clean up call. Intracom instance will no longer be usable once you call it.
This means it will likely exist at the top level, maybe even the main routine of your program. The intracom instance can be shared to many go routines for use but while keeping these in mind.
1. Whoever creates the Intracom instance should be the who calls `.Start()` and the one who calls `.Close()`
2. Because of #1, `.Start()` and `.Close()` are not thread-safe.
3. If you do `.Close()` too early and have a late `unsubscribe` or `unregister` follow that up, this would cause a **panic on a closed channel** but the bound function call just performed has a panic recovery implemented that will log at the ERROR level.
4. If you attempt to `.Register()` or `.Subscribe()` after a `.Close()` has been initiated, you will experience a panic. This is expected as you should NOT be trying to continue using an unusable intracom instance.

### Duplicate Register calls
Calling `.Register(<your-topic>)` against the same topic beyond the first time will always hand you back a reference to the already existing publishing channel as well as a callable function bound to that topic that when called will unregister that topic. Unregistering a topic will close all subscriber channels to that topic.

### Duplicate Subscription calls
Calling `.Subscribe(<your-subscriber-config>)` with the same config beyond the first time will always hand you back a reference to the already existing go channel as well as a callable function bound to that subscriber topic and consumer name that when called will unsubscribe that topic. Unsubscribing will close the specific subscriber channel for that consumer.

### Early Subscribers
Calling `.Subscribe(<your-subscriber-config>)` before a topic is registered will trigger a register to happen and immediately attach your subscriber to that topic. This helps when launching publishers and subscribers between separate go routines. Behind the scenes when `.Register(<topic>)` is called late it will re-act the same way as calling Register duplicate times. You will be given the already created publishing channel that was created during the early subscribers triggered register.

### Late Subscribers 
Late subscribers to a topic will always be given the LAST message published to the topic (if any have been published). 
So when joining as a new consumer you can be sure if there were any previous publishes, you will at least get the most recent published message.
