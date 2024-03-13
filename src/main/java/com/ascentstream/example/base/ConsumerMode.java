package com.ascentstream.example.base;

import com.ascentstream.example.utils.Contents;
import com.ascentstream.example.utils.PulsarUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

public class ConsumerMode {

    public static void consumerByExclusive(PulsarClient client, String topic) throws Exception {
        // 构造消费者
        Consumer<byte[]> consumer1 = client.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("sub-exclusive")
                .subscribe();
        // 构造生产者
        Producer<byte[]> producer = PulsarUtil.getPulsarProducer(client, topic);
        int numMessage = 10;
        for (int i = 0; i < numMessage; i++) {
            MessageId messageId = producer.send(("sync message " + i).getBytes());
            System.out.println("send sync message success, messageId(" + messageId + ")");
        }
        while (true) {
            Message<byte[]> message = consumer1.receive(3000, TimeUnit.MILLISECONDS);
            if (message == null) {
                break;
            }
            MessageId messageid = message.getMessageId();
            String data = new String(message.getData());
            System.out.println("receive  message messageId(" + messageid + ")" + ",value:" + data);
            consumer1.acknowledge(messageid);
        }
        try {
            //抛出异常
            Consumer<byte[]> consumer2 = client.newConsumer()
                    .topic(topic)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionName("sub-exclusive")
                    .subscribe();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    public static void consumerByFailover(PulsarClient client, String topic) throws PulsarClientException {
        // 构造消费者
        int numCousmers = 2;
        List<Consumer<byte[]>> consumers = new ArrayList<>();
        for (int i = 0; i < numCousmers; i++) {
            final Consumer<byte[]> consumer = client.newConsumer()
                    .topic(topic)
                    .subscriptionType(SubscriptionType.Failover)
                    .ackTimeout(3, TimeUnit.SECONDS)
                    .negativeAckRedeliveryDelay(10, TimeUnit.SECONDS)
                    .subscriptionName("sub-failover")
                    .subscribe();
            consumers.add(consumer);
        }
        // 构造生产者
        Producer<byte[]> producer = PulsarUtil.getPulsarProducer(client, topic);
        int numMessage = 10;
        for (int i = 0; i < numMessage; i++) {
            MessageId messageId = producer.send(("sync message " + i).getBytes());
            System.out.println("send sync message success, messageId(" + messageId + ")");
        }
        for (Consumer<byte[]> c : consumers) {
            while (true) {
                Message<byte[]> message = c.receive(3000, TimeUnit.MILLISECONDS);
                if (message == null) {
                    break;
                }
                MessageId messageId = message.getMessageId();
                String data = new String(message.getData());
                String topicName = message.getTopicName();
                System.out.println(
                        c.getConsumerName() + " receive " + topicName + " message messageId(" + messageId + ")"
                                + ",value:" + data);
                c.acknowledge(messageId);
            }
        }
    }

    public static void consumerByShared(PulsarClient client, String topic) throws PulsarClientException {
        // 构造消费者
        int numCousmers = 5;
        List<Consumer<byte[]>> consumers = new ArrayList<>();
        for (int i = 0; i < numCousmers; i++) {
            final Consumer<byte[]> consumer = client.newConsumer()
                    .topic(topic)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName("sub-shard")
                    .subscribe();
            consumers.add(consumer);
        }
        // 构造生产者
        Producer<byte[]> producer = PulsarUtil.getPulsarProducer(client, topic);
        int numMessage = 5;
        for (int i = 0; i < numMessage; i++) {
            MessageId messageId = producer.send(("sync message " + i).getBytes());
            System.out.println("send sync message success, messageId(" + messageId + ")");
        }
        for (Consumer<byte[]> c : consumers) {
            while (true) {
                Message<byte[]> message = c.receive(3000, TimeUnit.MILLISECONDS);
                if (message == null) {
                    break;
                }
                MessageId messageId = message.getMessageId();
                String data = new String(message.getData());
                String topicName = message.getTopicName();
                System.out.println(
                        c.getConsumerName() + " receive " + topicName + " message messageId(" + messageId + ")"
                                + ",value:" + data);
                c.acknowledge(messageId);
            }
        }
        consumers.forEach(consumer -> {
            try {
                consumer.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
    }

    public static void main(String[] args) throws Exception {
        PulsarClient client = PulsarUtil.getPulsarClient(Contents.SERVER_URL);
        String topic = "public/default/topic-exclusive";
        consumerByExclusive(client, topic);
        topic = "public/default/topic-failover";
        consumerByFailover(client, topic);
        topic = "public/default/topic-shared";
        consumerByShared(client, topic);
        client.close();
    }
}