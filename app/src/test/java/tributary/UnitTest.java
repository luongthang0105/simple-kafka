package tributary;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import tributary.core.Message;
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
        public void manualAllocation() {
            controller.createStringTopic("StringTopic");
            controller.createPartition("StringTopic", "partition1");
            controller.createPartition("StringTopic", "partition2");
            controller.createPartition("StringTopic", "partition3");
            controller.createStringProducer("producer1", "Manual");

            Message<String> message = new Message<String>("mess1", "partition1", "ahihihi");

            controller.produceMessage("producer1", "StringTopic", message, "partition1");
            assertEquals(1, controller.getPartitionSize("partition1"));

            controller.produceMessage("producer1", "StringTopic", message, "partition2");
            assertEquals(1, controller.getPartitionSize("partition2"));

            controller.produceMessage("producer1", "StringTopic", message, "partition3");
            assertEquals(1, controller.getPartitionSize("partition3"));
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
                    controller.createPartition("StringTopic", "partition1");
                    controller.createPartition("StringTopic", "partition2");
                    controller.createPartition("StringTopic", "partition3");
                    controller.createStringProducer("producer1", "Manual");

                    Message<String> message = new Message<String>("mess1", "partition1", "ahihihi");
                    controller.produceMessage("producer1", "StringTopic", message, "partition1");
                    controller.produceMessage("producer1", "StringTopic", message, "partition2");
                    controller.produceMessage("producer1", "StringTopic", message, "partition3");

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
                    assertEquals(1, controller.getNumConsumedMessages("partition1"));
                    assertEquals(1, controller.getNumConsumedMessages("partition2"));
                    assertEquals(1, controller.getNumConsumedMessages("partition3"));
                }

                @Test
                public void range() {
                    controller.createStringTopic("StringTopic");
                    controller.createPartition("StringTopic", "partition1");
                    controller.createPartition("StringTopic", "partition2");
                    controller.createPartition("StringTopic", "partition3");
                    controller.createPartition("StringTopic", "partition4");
                    controller.createPartition("StringTopic", "partition5");
                    controller.createPartition("StringTopic", "partition6");
                    controller.createPartition("StringTopic", "partition7");
                    controller.createPartition("StringTopic", "partition8");
                    controller.createStringProducer("producer1", "Manual");

                    Message<String> message = new Message<String>("mess1", "partition1", "ahihihi");

                    controller.produceMessage("producer1", "StringTopic", message, "partition1");
                    controller.produceMessage("producer1", "StringTopic", message, "partition2");
                    controller.produceMessage("producer1", "StringTopic", message, "partition3");
                    controller.produceMessage("producer1", "StringTopic", message, "partition4");
                    controller.produceMessage("producer1", "StringTopic", message, "partition5");
                    controller.produceMessage("producer1", "StringTopic", message, "partition6");
                    controller.produceMessage("producer1", "StringTopic", message, "partition7");
                    controller.produceMessage("producer1", "StringTopic", message, "partition8");

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
                    assertEquals(1, controller.getNumConsumedMessages("partition1"));
                    assertEquals(1, controller.getNumConsumedMessages("partition2"));
                    assertEquals(1, controller.getNumConsumedMessages("partition3"));
                    assertEquals(1, controller.getNumConsumedMessages("partition4"));
                    assertEquals(1, controller.getNumConsumedMessages("partition5"));
                    assertEquals(1, controller.getNumConsumedMessages("partition6"));
                    assertEquals(1, controller.getNumConsumedMessages("partition7"));
                    assertEquals(1, controller.getNumConsumedMessages("partition8"));
                }
            }
        }

        @Nested
        public class TestCannotConsumeFromUnassignedPartition {
            @Test
            public void roundRobin() {
                controller.createStringTopic("StringTopic");
                controller.createPartition("StringTopic", "partition1");
                controller.createPartition("StringTopic", "partition2");
                controller.createPartition("StringTopic", "partition3");
                controller.createStringProducer("producer1", "Manual");

                Message<String> message = new Message<String>("mess1", "partition1", "ahihihi");

                controller.produceMessage("producer1", "StringTopic", message, "partition1");
                controller.produceMessage("producer1", "StringTopic", message, "partition2");
                controller.produceMessage("producer1", "StringTopic", message, "partition3");

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
                assertEquals(0, controller.getNumConsumedMessages("partition1"));
                assertEquals(0, controller.getNumConsumedMessages("partition2"));
                assertEquals(0, controller.getNumConsumedMessages("partition3"));
            }

            @Test
            public void range() {
                controller.createStringTopic("StringTopic");
                controller.createPartition("StringTopic", "partition1");
                controller.createPartition("StringTopic", "partition2");
                controller.createPartition("StringTopic", "partition3");
                controller.createPartition("StringTopic", "partition4");
                controller.createPartition("StringTopic", "partition5");
                controller.createStringProducer("producer1", "Manual");

                Message<String> message = new Message<String>("mess1", "partition1", "ahihihi");

                controller.produceMessage("producer1", "StringTopic", message, "partition1");
                controller.produceMessage("producer1", "StringTopic", message, "partition2");
                controller.produceMessage("producer1", "StringTopic", message, "partition3");
                controller.produceMessage("producer1", "StringTopic", message, "partition4");
                controller.produceMessage("producer1", "StringTopic", message, "partition5");

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
                assertEquals(0, controller.getNumConsumedMessages("partition1"));
                assertEquals(0, controller.getNumConsumedMessages("partition2"));
                assertEquals(0, controller.getNumConsumedMessages("partition3"));
                assertEquals(0, controller.getNumConsumedMessages("partition4"));
                assertEquals(0, controller.getNumConsumedMessages("partition5"));
            }

        }

        @Test
        public void continueConsume() {
            controller.createStringTopic("StringTopic");
            controller.createPartition("StringTopic", "partition1");
            controller.createStringProducer("producer1", "Manual");

            Message<String> message = new Message<String>("mess1", "partition1", "ahihihi");

            controller.produceMessage("producer1", "StringTopic", message, "partition1");
            controller.produceMessage("producer1", "StringTopic", message, "partition1");
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

            assertEquals(2, controller.getNumConsumedMessages("partition1"));
        }
    }
}
