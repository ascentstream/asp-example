package com.ascentstream.example.advance;

import com.ascentstream.example.utils.Contents;
import com.ascentstream.example.utils.PulsarUtil;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;

public class ConsumerRetry {
    public static void unack(PulsarClient client, String topic) throws Exception {
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .negativeAckRedeliveryDelay(10, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sub_unack")
                .subscribe();
        Producer<byte[]> producer = PulsarUtil.getPulsarProducer(client, topic);
        // 同步发送消息
        System.out.println("send start");
        int numMessage = 2;
        for (int i = 0; i < numMessage; i++) {
            MessageId messageId = producer.newMessage()
                    .value(("sync message " + i).getBytes())
                    .send();
            System.out.println("send sync message success, messageId(" + messageId + ")");
        }

        for (int i = 0; i < numMessage * 2; i++) {
            Message<byte[]> message = consumer.receive();
            String topicName = message.getTopicName();
            MessageId messageId = message.getMessageId();
            String data = new String(message.getData());
            System.out.println(
                    PulsarUtil.getCurrentTime() + " receive " + topicName + " message messageId(" + messageId + ")"
                            + ",value:" + data);
            if (i < numMessage) {
                consumer.negativeAcknowledge(messageId);
            } else {
                consumer.acknowledge(messageId);
            }
        }

        numMessage = 4;
        for (int i = 0; i < numMessage; i++) {
            producer.sendAsync(("async message " + i).getBytes()).thenAccept(MessageId -> {
                System.out.println("send async message success, messageId(" + MessageId + ")");
            }).exceptionally(e -> {
                e.printStackTrace();
                return null;
            });
        }
        Thread.sleep(3000);
        for (int i = 0; i < numMessage + numMessage / 2; i++) {
            Message<byte[]> message = consumer.receive();
            String topicName = message.getTopicName();
            MessageId messageid = message.getMessageId();
            String data = new String(message.getData());
            System.out.println(
                    PulsarUtil.getCurrentTime() + " receive " + topicName + " message messageId(" + messageid + ")"
                            + ",value:" + data);
            if (i < numMessage) {
                if (i % 2 == 0) {
                    consumer.negativeAcknowledge(messageid);
                } else {
                    consumer.acknowledge(messageid);
                }
            } else {
                consumer.acknowledge(messageid);
            }
        }
    }

    public static void ackTimout(PulsarClient client, String topic) throws Exception {
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .ackTimeout(10, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sub_ackTimeout")
                .subscribe();
        Producer<byte[]> producer = PulsarUtil.getPulsarProducer(client, topic);
        // 同步发送消息
        System.out.println("send start");
        int numMessage = 2;
        for (int i = 0; i < numMessage; i++) {
            MessageId messageId = producer.newMessage()
                    .value(("sync message " + i).getBytes())
                    .send();
            System.out.println("send sync message success, messageId(" + messageId + ")");
        }

        for (int i = 0; i < numMessage * 2; i++) {
            Message message = consumer.receive();
            String topicName = message.getTopicName();
            MessageId messageId = message.getMessageId();
            String data = new String(message.getData());
            System.out.println(
                    PulsarUtil.getCurrentTime() + " receive " + topicName + " message messageId(" + messageId + ")"
                            + ",value:" + data);
            if (i < numMessage) {

            } else {
                consumer.acknowledge(messageId);
            }
        }
    }

    public static void reconsumeLater(PulsarClient client, String topic) throws Exception {
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .enableRetry(true)
                .subscriptionName("sub_reconsumeLater")
                .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(100)
                        .retryLetterTopic("").build())
                .subscribe();
        Producer<byte[]> producer = PulsarUtil.getPulsarProducer(client, topic);
        // 同步发送消息
        System.out.println("send start");
        int numMessage = 2;
        for (int i = 0; i < numMessage; i++) {
            MessageId messageId = producer.newMessage()
                    .value(("sync message " + i).getBytes())
                    .send();
            System.out.println("send sync message success, messageId(" + messageId + ")");
        }

        for (int i = 0; i < numMessage * 2; i++) {
            Message message = consumer.receive();
            String topicName = message.getTopicName();
            MessageId messageid = message.getMessageId();
            String data = new String(message.getData());
            System.out.println(PulsarUtil.getCurrentTime() + " receive " + topicName + " message messageId(" + messageid
                    + ")" + ",value:" + data + ",properties" + message.getProperties());
            if (i < numMessage) {
                consumer.reconsumeLater(message, 10, TimeUnit.SECONDS);
            } else {
                consumer.acknowledge(messageid);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String topic = "persistent://public/default/topic";
        PulsarClient client = PulsarUtil.getPulsarClient(Contents.SERVER_URL);
        unack(client, topic);
        ackTimout(client, topic);
        reconsumeLater(client, topic);
        client.close();
    }
}
