package com.ascentstream.example.advance;

import com.ascentstream.example.utils.Contents;
import com.ascentstream.example.utils.PulsarUtil;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

public class ChunkingMessage {
    private static String createMessagePayload(int size) {
        StringBuilder str = new StringBuilder();
        Random rand = new Random();
        for (int i = 0; i < size; i++) {
            str.append(rand.nextInt(10));
        }
        return str.toString();
    }

    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        PulsarClient client = PulsarUtil.getPulsarClient(Contents.SERVER_URL);
        String topic = "public/default/topic-chunking";
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("sub_chunking")
                .subscribe();

        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic(topic)
                .enableChunking(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .maxPendingMessages(10240)
                .sendTimeout(3000, TimeUnit.MILLISECONDS)
                .create();
        int numMessage = 1;
        String value = createMessagePayload(1024 * 1024 * 20);
        long startTime = System.currentTimeMillis();
        producer.newMessage()
                .value(value.getBytes())
                .sendAsync().thenAccept(messageId -> {
                    long useTime = System.currentTimeMillis() - startTime;
                    System.out.println(messageId + "，" + useTime);
                }).exceptionally(ex -> {
                    ex.printStackTrace();
                    return null;
                });


        while (true) {
            Message message = consumer.receive();
            if (message == null) {
                break;
            }
            MessageId messageId = message.getMessageId();
            String data = new String(message.getData());
            String topicName = message.getTopicName();
            System.out.println("receive " + topicName + " message messageId(" + messageId + ")" + ",value:" +
                    message.getData().length / 1024 / 1024);
            consumer.acknowledge(messageId);
        }
        client.close();
    }
}
