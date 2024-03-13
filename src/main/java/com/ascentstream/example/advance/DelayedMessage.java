package com.ascentstream.example.advance;

import com.ascentstream.example.utils.Contents;
import com.ascentstream.example.utils.PulsarUtil;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

public class DelayedMessage {

    public static void deliverAfter(PulsarClient client, String topic) throws PulsarClientException {
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sub_delay")
                .subscribe();
        Producer<byte[]> producer = PulsarUtil.getPulsarProducer(client, topic);
        // 同步发送消息
        System.out.println("send start");
        int numMessages = 2;
        for (int i = 0; i < numMessages; i++) {
            MessageId messageId = producer.newMessage()
                    .value(("sync message " + i).getBytes())
                    .deliverAfter(10, TimeUnit.SECONDS)
                    .send();
            System.out.println(
                    PulsarUtil.getCurrentTime() + " send sync message success, messageId(" + messageId + ")");
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
    }

    public static void deliverAt(PulsarClient client, String topic) throws Exception {
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sub_delay")
                .subscribe();
        Producer<byte[]> producer = PulsarUtil.getPulsarProducer(client, topic);
        // 同步发送消息
        System.out.println("send start");
        int numMessages = 2;
        for (int i = 0; i < numMessages; i++) {
            MessageId messageId = producer.newMessage()
                    .value(("sync message " + i).getBytes())
                    .deliverAt(PulsarUtil.TimeToStamp("2022-03-29 11:08:00"))
                    .send();
            System.out.println(
                    PulsarUtil.getCurrentTime() + " send sync message success, messageId(" + messageId + ")");
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
    }

    public static void main(String[] args) throws Exception {
        String topic = "persistent://public/default/delay";
        PulsarClient client = PulsarUtil.getPulsarClient(Contents.SERVER_URL);
        deliverAfter(client, topic);
        deliverAt(client, topic);
    }
}
