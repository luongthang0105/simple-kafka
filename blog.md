# Tributary - A Simplified Apache Kafka

## Preliminary Design

### Analysis of Engineering Requirements

The whole system is actually not very complicated if we proceed to model.

Based on the engineering requirements, we can come up with some of the core classes:

1. Producer
2. Consumer

3. ConsumerGroup

   - contains a number of consumers

4. Partition
5. Topic
   - contains a number of partitions
6. Event
   - This might be an interface? Since we want a flexible way to store different types of events, maybe make this an interface? Still not finalised yet tho
7. Message

Other than that, we think that it may be very useful have a that represents the rebalancing and allocation strategies. We may lean towards the decision of applying the **_Strategy Pattern_** for these strategies.

Further analysis in the domain modelling of the specs will be done in the UML diagram.

Other than that, maybe we'll go into somewhat in-depth analysis of what are these classes doing.

We may have a **TributaryController** which manages everything: consumers, producers, and partitions.

Each producer has its own allocation strategy, and may know how to:

- send event to a partition based on the key encapsulated in the message
- allocate event to a random partition

Each consumer group has its own rebalancing strategy and will reallocate partitions to their consumers after adding/deleting a consumer.

Consumers obviously should be able to consume events and replay events as well. This means we will probably need to store an internal offset/index (something kind of like a file pointer in File Systems) in order to keep track of where we left off, or replay by changing the offset. Consumers will also need to store these messages.

Topic contains a number of partitions and each partition may also have an internal index which points to keep track where they left off. Partitions need to save themselves a list of messages as well, so they can support playback.

A message is probably composed of a payload type, id, time created, key which indicates partition, and value which is an object referring to the information of a topic.

### Usability Tests List

List of scenarios:

1. Simple system with 1 producer, 1 topic with 1 partition, 1 consumer group with 1 consumer.
   - Test that a message lifecycle can be completed
   - Check that the id and contents of the message is correct
2. System with 2 producers, 2 topics with 1 partition each (1 of type Integer, 1 of type String), 2 consumer groups with 1 consumer each.
   - Test that messages of type Integer/String can be consumed correctly
3. System with 3 producers, 1 topics with 3 partitions, 1 consumer group with 3 consumers.
   - Test that parallel producing events can be handled by the topics correctly. We check this by calling a cli command "show topic"
4. System with 3 producers, 1 topics with 3 partitions, 1 consumer group with 3 consumers.
   - Test that consuming events parallel is handle by 3 consumers correctly.

### Initial UML Diagram

[Intial UML Diagram](InitialTributaryUML.pdf)

### Testing Plan

2. Unit tests on individual components:

   2.1. Producer:

   a) Allocation of Messages to Partitions as a manual producer

   - Test that the producer requests the Tributary system to assign a message to a particular partition by providing its corresponding key.
   - Make sure the key of the given message identical to the Id of the assigned partition.

   These tests should ensure that once a producer has been created with the given method, it cannot change its message allocation method.

   2.2. Consumer Group:

   a) Consumer group consumes a topic with the corresponding rebalancing strategy:

   - Test that every consumers in one specific consumer group can consume all the partitions in the assigned topic
   - Test that every consumers in one specific consumer group cannot consume any partitions in the unassigned topic
   - Test that When a new consumer group is created, the consumers in the group begin their consumption from the first unconsumed message in all of the topics partitions they are assigned to.
   - all consumers that share a partition consume messages parallel to each other.

   b) Playback:

   - Trigger a controlled replay of a partition that hasn't got any messages yet.
   - Add some messages to a partition. A consumer starting at offset 6 that performed normal consumption until offset 9. This consumer then triggered a controlled replay from offset 4 that played back all the messages from that offset until the most recently consumed message.Test that messages 6, 7, 8 and 9 were consumed again.

3. Unit tests on design considerations:

   3.1 Concurrency:

   - Test correctness with the scenerio that Producers and Consumers are all running in parallel, operating on a shared resource.

     3.2 Generics:

   - Ensure that an object of any type can be used as an event payload in a tributary topic.

4. Integration Test:

   This integration test should make sure that:

   a) Command line succeeds and leads to the right corresponding action, and exits when fails

   b) Operate on the producers alone should ensure correctness

   c) Operate on the consumers alone should ensure correctness

   d) Operate on both the producers and consumers concurrently should ensure correctness
