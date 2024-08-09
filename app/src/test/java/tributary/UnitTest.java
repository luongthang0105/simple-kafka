package tributary;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.FileAlreadyExistsException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import tributary.core.Message;
import tributary.core.TributaryController;
import tributary.core.input.ConsumeInput;
import tributary.core.input.ProduceInput;

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

    @Nested
    public class ParallelTests {
        @Nested
        public class ParallelProduce {
            @Test
            public void oneProducerToManyPartition() throws InterruptedException {
                controller.createStringTopic("StringTopic");
                controller.createPartition("StringTopic", "partition1");
                controller.createPartition("StringTopic", "partition2");
                controller.createPartition("StringTopic", "partition3");
                controller.createStringProducer("producer1", "Manual");

                Message<String>[] messages = new Message[3];
                messages[0] = new Message<String>("mess1", "partition1", "ahihihi");
                messages[1] = new Message<String>("mess2", "partition2", "ahihihi");
                messages[2] = new Message<String>("mess3", "partition3", "ahihihi");

                ProduceInput[] inputs = new ProduceInput[3];
                inputs[0] = new ProduceInput("producer1", "StringTopic", messages[0]);
                inputs[1] = new ProduceInput("producer1", "StringTopic", messages[1]);
                inputs[2] = new ProduceInput("producer1", "StringTopic", messages[2]);

                controller.parallelProduce(inputs);

                Thread.sleep(100);
                // Check whether each partition has 1 message;
                assertEquals(1, controller.getPartitionSize("partition1"));
                assertEquals(1, controller.getPartitionSize("partition2"));
                assertEquals(1, controller.getPartitionSize("partition3"));

                controller.createConsumerGroup("group1", "StringTopic", "RoundRobin");
                controller.createConsumer("group1", "consumer1");
                controller.createConsumer("group1", "consumer2");
                controller.createConsumer("group1", "consumer3");

                // In this case, each consumer should consume from 1 partition, bc we're using
                // RoundRobin strat
                controller.consumeMessage("consumer1", "partition1");
                controller.consumeMessage("consumer2", "partition2");
                controller.consumeMessage("consumer3", "partition3");

                // Check if all messages were consumed
                assertEquals(1, controller.getNumConsumedMessages("partition1"));
                assertEquals(1, controller.getNumConsumedMessages("partition2"));
                assertEquals(1, controller.getNumConsumedMessages("partition3"));
            }

            @Test
            public void manyProducerToOnePartition() throws InterruptedException {
                controller.createStringTopic("StringTopic");
                controller.createPartition("StringTopic", "partition1");
                controller.createStringProducer("producer1", "Manual");
                controller.createStringProducer("producer2", "Manual");
                controller.createStringProducer("producer3", "Manual");

                Message<String>[] messages = new Message[3];
                messages[0] = new Message<String>("mess1", "partition1", "ahihihi");
                messages[1] = new Message<String>("mess2", "partition1", "ahihihi");
                messages[2] = new Message<String>("mess3", "partition1", "ahihihi");

                ProduceInput[] inputs = new ProduceInput[3];
                inputs[0] = new ProduceInput("producer1", "StringTopic", messages[0]);
                inputs[1] = new ProduceInput("producer2", "StringTopic", messages[1]);
                inputs[2] = new ProduceInput("producer3", "StringTopic", messages[2]);

                controller.parallelProduce(inputs);
                Thread.sleep(500);

                // Check whether partition1 has 3 messages
                assertEquals(3, controller.getPartitionSize("partition1"));

                controller.createConsumerGroup("group1", "StringTopic", "RoundRobin");
                controller.createConsumer("group1", "consumer1");

                controller.consumeMessages("consumer1", "partition1", 3);

                // Check if all messages were consumed
                assertEquals(3, controller.getNumConsumedMessages("partition1"));
            }
        }

        @Nested
        public class ParallelConsume {
            @Test
            public void oneConsumerToManyPartition() throws InterruptedException {
                controller.createStringTopic("StringTopic");
                controller.createPartition("StringTopic", "partition1");
                controller.createPartition("StringTopic", "partition2");
                controller.createPartition("StringTopic", "partition3");
                controller.createStringProducer("producer1", "Manual");

                Message<String>[] messages = new Message[3];
                messages[0] = new Message<String>("mess1", "partition1", "ahihihi");
                messages[1] = new Message<String>("mess2", "partition2", "ahihihi");
                messages[2] = new Message<String>("mess3", "partition3", "ahihihi");

                ProduceInput[] inputs = new ProduceInput[3];
                inputs[0] = new ProduceInput("producer1", "StringTopic", messages[0]);
                inputs[1] = new ProduceInput("producer1", "StringTopic", messages[1]);
                inputs[2] = new ProduceInput("producer1", "StringTopic", messages[2]);

                controller.parallelProduce(inputs);

                controller.createConsumerGroup("group1", "StringTopic", "Range");
                controller.createConsumer("group1", "consumer1");

                ConsumeInput[] consumeInputs = new ConsumeInput[3];
                consumeInputs[0] = new ConsumeInput("consumer1", "partition1");
                consumeInputs[1] = new ConsumeInput("consumer1", "partition2");
                consumeInputs[2] = new ConsumeInput("consumer1", "partition3");

                controller.parallelConsume(consumeInputs);

                Thread.sleep(500);
                // Check if all messages were consumed
                assertEquals(1, controller.getNumConsumedMessages("partition1"));
                assertEquals(1, controller.getNumConsumedMessages("partition2"));
                assertEquals(1, controller.getNumConsumedMessages("partition3"));
            }

            @Test
            public void manyConsumerToOnePartition() throws InterruptedException {
                controller.createStringTopic("StringTopic");
                controller.createPartition("StringTopic", "partition1");
                controller.createStringProducer("producer1", "Manual");

                Message<String>[] messages = new Message[6];
                messages[0] = new Message<String>("mess1", "partition1", "ahihihi");
                messages[1] = new Message<String>("mess2", "partition1", "ahihihi");
                messages[2] = new Message<String>("mess3", "partition1", "ahihihi");
                messages[3] = new Message<String>("mess3", "partition1", "ahihihi");
                messages[4] = new Message<String>("mess3", "partition1", "ahihihi");
                messages[5] = new Message<String>("mess3", "partition1", "ahihihi");

                ProduceInput[] inputs = new ProduceInput[6];
                inputs[0] = new ProduceInput("producer1", "StringTopic", messages[0]);
                inputs[1] = new ProduceInput("producer1", "StringTopic", messages[1]);
                inputs[2] = new ProduceInput("producer1", "StringTopic", messages[2]);
                inputs[3] = new ProduceInput("producer1", "StringTopic", messages[3]);
                inputs[4] = new ProduceInput("producer1", "StringTopic", messages[4]);
                inputs[5] = new ProduceInput("producer1", "StringTopic", messages[5]);

                controller.parallelProduce(inputs);

                Thread.sleep(100);
                controller.createConsumerGroup("group1", "StringTopic", "Range");
                controller.createConsumer("group1", "consumer1");

                controller.createConsumerGroup("group2", "StringTopic", "Range");
                controller.createConsumer("group2", "consumer2");

                controller.createConsumerGroup("group3", "StringTopic", "Range");
                controller.createConsumer("group3", "consumer3");

                ConsumeInput[] consumeInputs = new ConsumeInput[6];
                consumeInputs[0] = new ConsumeInput("consumer1", "partition1");
                consumeInputs[1] = new ConsumeInput("consumer2", "partition1");
                consumeInputs[2] = new ConsumeInput("consumer3", "partition1");
                consumeInputs[3] = new ConsumeInput("consumer1", "partition1");
                consumeInputs[4] = new ConsumeInput("consumer2", "partition1");
                consumeInputs[5] = new ConsumeInput("consumer3", "partition1");

                controller.parallelConsume(consumeInputs);

                Thread.sleep(100);
                // Check if all messages were consumed
                assertEquals(6, controller.getNumConsumedMessages("partition1"));
            }

            @Test
            public void manyConsumerToManyPartition() throws InterruptedException {
                controller.createStringTopic("StringTopic");
                controller.createPartition("StringTopic", "partition1");
                controller.createPartition("StringTopic", "partition2");
                controller.createStringProducer("producer1", "Manual");

                Message<String>[] messages = new Message[4];
                messages[0] = new Message<String>("mess1", "partition1", "ahihihi");
                messages[1] = new Message<String>("mess2", "partition1", "ahihihi");
                messages[2] = new Message<String>("mess3", "partition2", "ahihihi");
                messages[3] = new Message<String>("mess4", "partition2", "ahihihi");

                ProduceInput[] inputs = new ProduceInput[4];
                inputs[0] = new ProduceInput("producer1", "StringTopic", messages[0]);
                inputs[1] = new ProduceInput("producer1", "StringTopic", messages[1]);
                inputs[2] = new ProduceInput("producer1", "StringTopic", messages[2]);
                inputs[3] = new ProduceInput("producer1", "StringTopic", messages[3]);

                controller.parallelProduce(inputs);

                Thread.sleep(100);
                controller.createConsumerGroup("group1", "StringTopic", "RoundRobin");
                controller.createConsumer("group1", "g1-consumer1");
                controller.createConsumer("group1", "g1-consumer2");

                controller.createConsumerGroup("group2", "StringTopic", "RoundRobin");
                controller.createConsumer("group2", "g2-consumer1");
                controller.createConsumer("group2", "g2-consumer2");

                ConsumeInput[] consumeInputs = new ConsumeInput[4];
                consumeInputs[0] = new ConsumeInput("g1-consumer1", "partition1");
                consumeInputs[1] = new ConsumeInput("g1-consumer2", "partition2");
                consumeInputs[2] = new ConsumeInput("g2-consumer1", "partition1");
                consumeInputs[3] = new ConsumeInput("g2-consumer2", "partition2");

                controller.parallelConsume(consumeInputs);

                Thread.sleep(100);
                // Check if all messages were consumed
                assertEquals(2, controller.getNumConsumedMessages("partition1"));
                assertEquals(2, controller.getNumConsumedMessages("partition2"));
            }
        }
    }

    @Nested
    public class Generics {
        @Test
        public void typeDouble() {
            controller.createTopic("Doublesss", Double.class);
            controller.createPartition("Doublesss", "partition1");
            controller.createPartition("Doublesss", "partition2");
            controller.createPartition("Doublesss", "partition3");
            controller.createProducer("producer1", "Manual", Double.class);

            Message<Double> message = new Message<Double>("mess1", "partition1", 1.0);

            controller.produceMessage("producer1", "Doublesss", message, "partition1");
            controller.produceMessage("producer1", "Doublesss", message, "partition2");
            controller.produceMessage("producer1", "Doublesss", message, "partition3");

            controller.createConsumerGroup("group1", "Doublesss", "RoundRobin");
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
        public void typeExceptions() {
            controller.createTopic("Exceptions", Exception.class);
            controller.createPartition("Exceptions", "partition1");
            controller.createPartition("Exceptions", "partition2");
            controller.createPartition("Exceptions", "partition3");
            controller.createProducer("producer1", "Manual", Exception.class);

            Message<Exception> message1 = new Message<Exception>("mess1", "partition1",
                    new InterruptedException("exception"));
            Message<Exception> message2 = new Message<Exception>("mess1", "partition1",
                    new FileAlreadyExistsException("exception"));
            Message<Exception> message3 = new Message<Exception>("mess1", "partition1",
                    new NullPointerException("exception"));

            controller.produceMessage("producer1", "Exceptions", message1, "partition1");
            controller.produceMessage("producer1", "Exceptions", message2, "partition2");
            controller.produceMessage("producer1", "Exceptions", message3, "partition3");

            controller.createConsumerGroup("group1", "Exceptions", "RoundRobin");
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
    }

    @Nested
    public class TestPlayBack {

    }
}
