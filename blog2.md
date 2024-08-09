# Tributary - A Simplified Apache Kafka

## Final Design

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

### Final UML Diagram

[Final UML Diagram](FinalUML.pdf)

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

### Overview of the Design Patterns

- For handling multiple rebalancing methods and allocate methods, we use Strategy Pattern.
- We use has-a relationships for every classes in the Tributary system.

### A brief reflection on the assignment

- We first found it hard to keep the design simple and loosly-coupled since the customer, producer and partition works around each other, so we tended to save a lot of unnecessary fields for these 3 classes.

- We found it hard to split the list of tuples for the parallel function since the initial design is ((a,b,c), (d,r,f)) so we try to convert it to somehting like ((a,b,c)|(d,e,f))

- We found it hard to find a way for the parallel function to synchronise and ensure the there's no race conditions between threads. But at the end, we realised that if the Consumer instances are operating on different Partition objects, the synchronization on the method will be on a per-object basis.

- We needed a response kind of class to handle all the output messages to the CLI, but we ran out of time for doing so.
