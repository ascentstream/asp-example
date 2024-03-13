package com.ascentstream.example.base;

import com.ascentstream.example.utils.Contents;
import com.ascentstream.example.utils.PulsarUtil;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicMetadata;

public class RoutingMode {

    public static void roundRobinPartition(PulsarClient client, String topic) throws PulsarClientException {
        Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sub_roundRobinPartition")
                .subscribe();
        // 同步发送消息
        int numMessages = 10;
        for (int i = 0; i < numMessages; i++) {
            MessageId messageId = producer.newMessage()
                    .value(("sync message " + i).getBytes())
                    .send();
            System.out.println("send sync message success, messageId(" + messageId + ")");
        }

        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> message = consumer.receive();
            String topicName = message.getTopicName();
            MessageId messageid = message.getMessageId();
            String data = new String(message.getData());
            System.out.println(
                    PulsarUtil.getCurrentTime() + " receive " + topicName + " message messageId(" + messageid + ")"
                            + ",value:" + data);
            consumer.acknowledge(messageid);
        }
        consumer.close();
        producer.close();
    }

    public static void singlePartition(PulsarClient client, String topic) throws PulsarClientException {
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic(topic)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sub_singlePartition")
                .subscribe();
        // 同步发送消息
        int numMessages = 10;
        for (int i = 0; i < numMessages; i++) {
            MessageId messageId = producer.newMessage()
                    .value(("sync message " + i).getBytes())
                    .send();
            System.out.println("send sync message success, messageId(" + messageId + ")");
        }

        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> message = consumer.receive();
            String topicName = message.getTopicName();
            MessageId messageid = message.getMessageId();
            String data = new String(message.getData());
            System.out.println(
                    PulsarUtil.getCurrentTime() + " receive " + topicName + " message messageId(" + messageid + ")"
                            + ",value:" + data);
            consumer.acknowledge(messageid);
        }
        consumer.close();
        producer.close();
    }

    public static void customPartition(PulsarClient client, String topic) throws PulsarClientException {
        Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(new MessageRouter() {
                    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                        return 1;
                    }
                }).create();
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sub_customPartition")
                .subscribe();
        // 同步发送消息
        int numMessage = 10;
        for (int i = 0; i < numMessage; i++) {
            MessageId messageId = producer.newMessage()
                    .value(("sync message " + i).getBytes())
                    .send();
            System.out.println("send sync message success, messageId(" + messageId + ")");
        }
        for (int i = 0; i < numMessage; i++) {
            Message<byte[]> message = consumer.receive();
            String topicName = message.getTopicName();
            MessageId messageid = message.getMessageId();
            String data = new String(message.getData());
            System.out.println(
                    PulsarUtil.getCurrentTime() + " receive " + topicName + " message messageId(" + messageid + ")"
                            + ",value:" + data);
            consumer.acknowledge(messageid);
        }
        consumer.close();
        producer.close();
    }

    public static void keyPartition(PulsarClient client, String topic) throws PulsarClientException {
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic(topic)
                .create();
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sub_keyPartition")
                .subscribe();
        // 同步发送消息
        int numMessage = 10;
        int NUMBER_OF_KEYS = 3;
        for (int i = 0; i < numMessage; i++) {
            String key = "mykey-" + (i % NUMBER_OF_KEYS);
            MessageId messageId = producer.newMessage()
                    .value(("sync message " + i).getBytes())
                    .key(key)
                    .send();
            System.out.println("send sync message success, messageId(" + messageId + ")");
        }

        while (true) {
            Message<byte[]> message = consumer.receive(3, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            String topicName = message.getTopicName();
            MessageId messageid = message.getMessageId();
            String data = new String(message.getData());
            System.out.println(PulsarUtil.getCurrentTime() + " receive " + topicName + " message messageId("
                    + messageid + ")" + ",value:" + data + ",key:" + message.getKey());
            consumer.acknowledge(messageid);
        }
        consumer.close();
        producer.close();
    }

    public static void main(String[] args) throws Exception {
        String topic = "persistent://public/default/topic";
        PulsarClient client = PulsarUtil.getPulsarClient(Contents.SERVER_URL);
        roundRobinPartition(client, topic);
        singlePartition(client, topic);
        customPartition(client, topic);
        keyPartition(client, topic);
        client.close();
    }


}
