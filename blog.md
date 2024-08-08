Task 1
Testing Plan:
1. Unit tests on the Tributary command line interface:
- Test that every command lines corresponds to the right action.

2. Unit tests on individual components:

2.1. Producer:

a) Allocation of Messages to Partitions as a random producer:
- Test that the producer requests the Tributary system to randomly assign a message to a partition. Test the randomness using seeds.

b) Allocation of Messages to Partitions as a manual producer
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
- This integration test should make sure that:

a) Command line succeeds and leads to the right corresponding action, and exits when fails
b) Operate on the producers alone should ensure correctness 
c) Operate on the consumers alone should ensure correctness
d) Operate on both the producers and consumers concurrently should ensure correctness





