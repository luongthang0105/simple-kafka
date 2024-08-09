package tributary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import tributary.core.TributaryController;

public class UnitTest {
    private TributaryController controller;

    @BeforeEach
    public void beforeEach() {
        controller = new TributaryController();
    }

    @Nested
    public class Producer {
        @Test
        public void randomAllocation() {
            controller.createStringTopic("StringTopic");
            // Test randomness by checking that its does not leave out any spots?
        }

        @Test
        public void manualAllocation() {
            controller.createStringTopic("StringTopic");
            controller.createPartition("StringTopic", "part1");
            controller.createPartition("StringTopic", "part2");
            controller.createPartition("StringTopic", "part3");
            controller.createStringProducer("producer1", "Manual");

            controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition1");
            assertEquals(1, controller.getPartitionSize("StringTopic", "partition1"));

            controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition2");
            assertEquals(1, controller.getPartitionSize("StringTopic", "partition2"));

            controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition3");
            assertEquals(1, controller.getPartitionSize("StringTopic", "partition3"));
        }
    }

    @Nested
    public class Consumer {
        @Nested
        public class ConsumeEvents {
            @Nested
            public class TestConsumeAllPartitions {
                @Test
                public void roundRobin() {
                    controller.createStringTopic("StringTopic");
                    controller.createPartition("StringTopic", "part1");
                    controller.createPartition("StringTopic", "part2");
                    controller.createPartition("StringTopic", "part3");
                    controller.createStringProducer("producer1", "Manual");

                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition1");
                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition2");
                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition3");

                    controller.createConsumerGroup("group1", "StringTopic", "RoundRobin");
                    controller.createConsumer("group1", "consumer1");
                    controller.createConsumer("group1", "consumer2");
                    controller.createConsumer("group1", "consumer3");

                    // In this case, each consumer should consume from 1 partition, bc we're using
                    // RoundRobin strat
                    controller.consumeMessage("consumer1", "partition1");
                    controller.consumeMessage("consumer2", "partition2");
                    controller.consumeMessage("consumer3", "partition3");

                    // Check if all messages are consumed by consumers
                    assertEquals(1, controller.getNumConsumedMessages("StringTopic", "partition1"));
                    assertEquals(1, controller.getNumConsumedMessages("StringTopic", "partition2"));
                    assertEquals(1, controller.getNumConsumedMessages("StringTopic", "partition3"));
                }

                @Test
                public void range() {
                    controller.createStringTopic("StringTopic");
                    controller.createPartition("StringTopic", "part1");
                    controller.createPartition("StringTopic", "part2");
                    controller.createPartition("StringTopic", "part3");
                    controller.createStringProducer("producer1", "Manual");

                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition1");
                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition2");
                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition3");
                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition4");
                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition5");
                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition6");
                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition7");
                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition8");

                    controller.createConsumerGroup("group1", "StringTopic", "Range");
                    controller.createConsumer("group1", "consumer1");
                    controller.createConsumer("group1", "consumer2");
                    controller.createConsumer("group1", "consumer3");

                    // In this case, consumer1 and consumer2 will consume 3 partitions bc of Range
                    // strat.
                    // consumer1 consumes from partition1,2,7
                    // consumer2 consumes from partition3,4,8
                    // consumer3 consumes from partition5,6

                    controller.consumeMessage("consumer1", "partition1");
                    controller.consumeMessage("consumer1", "partition2");
                    controller.consumeMessage("consumer1", "partition7");

                    controller.consumeMessage("consumer2", "partition3");
                    controller.consumeMessage("consumer2", "partition4");
                    controller.consumeMessage("consumer2", "partition8");

                    controller.consumeMessage("consumer3", "partition5");
                    controller.consumeMessage("consumer3", "partition6");

                    // Check if all messages are consumed by consumers
                    assertEquals(1, controller.getNumConsumedMessages("StringTopic", "partition1"));
                    assertEquals(1, controller.getNumConsumedMessages("StringTopic", "partition2"));
                    assertEquals(1, controller.getNumConsumedMessages("StringTopic", "partition3"));
                    assertEquals(1, controller.getNumConsumedMessages("StringTopic", "partition4"));
                    assertEquals(1, controller.getNumConsumedMessages("StringTopic", "partition5"));
                    assertEquals(1, controller.getNumConsumedMessages("StringTopic", "partition6"));
                    assertEquals(1, controller.getNumConsumedMessages("StringTopic", "partition7"));
                    assertEquals(1, controller.getNumConsumedMessages("StringTopic", "partition8"));
                }
            }

            @Nested
            public class TestCannotConsumeFromUnassignedPartition {
                @Test
                public void roundRobin() {
                    controller.createStringTopic("StringTopic");
                    controller.createPartition("StringTopic", "part1");
                    controller.createPartition("StringTopic", "part2");
                    controller.createPartition("StringTopic", "part3");
                    controller.createStringProducer("producer1", "Manual");

                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition1");
                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition2");
                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition3");

                    controller.createConsumerGroup("group1", "StringTopic", "RoundRobin");
                    controller.createConsumer("group1", "consumer1");
                    controller.createConsumer("group1", "consumer2");
                    controller.createConsumer("group1", "consumer3");

                    // In this case, each consumer should consume from 1 partition, bc we're using
                    // RoundRobin strat
                    controller.consumeMessage("consumer1", "partition3");
                    controller.consumeMessage("consumer2", "partition1");
                    controller.consumeMessage("consumer3", "partition2");

                    // Check if all messages couldn't be consumed
                    assertEquals(0, controller.getNumConsumedMessages("StringTopic", "partition1"));
                    assertEquals(0, controller.getNumConsumedMessages("StringTopic", "partition2"));
                    assertEquals(0, controller.getNumConsumedMessages("StringTopic", "partition3"));
                }

                @Test
                public void range() {
                    controller.createStringTopic("StringTopic");
                    controller.createPartition("StringTopic", "part1");
                    controller.createPartition("StringTopic", "part2");
                    controller.createPartition("StringTopic", "part3");
                    controller.createStringProducer("producer1", "Manual");

                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition1");
                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition2");
                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition3");
                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition4");
                    controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition5");

                    controller.createConsumerGroup("group1", "StringTopic", "Range");
                    controller.createConsumer("group1", "consumer1");
                    controller.createConsumer("group1", "consumer2");
                    controller.createConsumer("group1", "consumer3");

                    // In this case, consumer1 and consumer2 will consume 2 partitions bc of Range
                    // strat.
                    // consumer1 consumes from partition1,4
                    // consumer2 consumes from partition2,5
                    controller.consumeMessage("consumer1", "partition2");
                    controller.consumeMessage("consumer1", "partition3");

                    controller.consumeMessage("consumer2", "partition1");
                    controller.consumeMessage("consumer2", "partition4");

                    controller.consumeMessage("consumer3", "partition5");

                    // Check if all partitions does not have any message consumed
                    assertEquals(0, controller.getNumConsumedMessages("StringTopic", "partition1"));
                    assertEquals(0, controller.getNumConsumedMessages("StringTopic", "partition2"));
                    assertEquals(0, controller.getNumConsumedMessages("StringTopic", "partition3"));
                    assertEquals(0, controller.getNumConsumedMessages("StringTopic", "partition4"));
                    assertEquals(0, controller.getNumConsumedMessages("StringTopic", "partition5"));
                }

            }

            @Test
            public void continueConsume() {
                controller.createStringTopic("StringTopic");
                controller.createPartition("StringTopic", "part1");
                controller.createStringProducer("producer1", "Manual");

                controller.produceMessage("producer1", "StringTopic", "ahihihi", "partition1");
                controller.produceMessage("producer1", "StringTopic", "ahihihi2", "partition1");

                // Group1 
                controller.createConsumerGroup("group1", "StringTopic", "Range");
                controller.createConsumer("group1", "g1-consumer1");

                // Consumer1 from group1 consume a message from partition1
                controller.consumeMessage("g1-consumer1", "partition1");

                // Group2
                controller.createConsumerGroup("group2", "StringTopic", "Range");
                controller.createConsumer("group2", "g2-consumer1");

                // Consumer1 from group2 consume a message from partition1
                controller.consumeMessage("g2-consumer1", "partition1");

                assertEquals(2, controller.getNumConsumedMessages("StringTopic", "partition1"));

                List<String> consumedMessages = controller.getConsumedMessages("group2", "g2-consumer1");
                assertTrue(consumedMessages.get(0).equals("ahihihi2"));
            }
        }
    }
}
