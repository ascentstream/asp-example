package com.ascentstream.example.base;//package com.sn.trainning.day1.ex1_basics;

import com.ascentstream.example.utils.Contents;
import com.ascentstream.example.utils.PulsarUtil;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

public class ComplexProducer {

    //测试分区Topic设置key
    public static void sendMessageByKey(PulsarClient client) throws PulsarClientException {
        // 分区Topic
        String topic = "public/default/topic-key";
        // 构造消费者
        Consumer<byte[]> consumer = PulsarUtil.getPulsarConsumer(client, topic, "sub_key");
        // 构造生产者
        Producer<byte[]> producer = PulsarUtil.getPulsarProducer(client, topic);
        int numMessage = 10;
        for (int i = 0; i < numMessage; i++) {
            String key = "mykey-" + i;
            String value = "myvalue-" + i;
            producer.newMessage()
                    .key(key)
                    .value(value.getBytes())
                    .send();
        }
        //指定一样的key, 验证是否会发送到同一个分区Topic里
        for (int i = 0; i < 3; i++) {
            String key = "mykey-" + 0;
            String value = "myvalue-" + i;
            producer.newMessage()
                    .key(key)
                    .value(value.getBytes())
                    .send();
        }
        numMessage += 3;
        System.out.println("send " + numMessage + " num message success!");

        for (int i = 0; i < numMessage; i++) {
            Message<byte[]> message = consumer.receive(3000, TimeUnit.MILLISECONDS);
            String topicName = message.getTopicName();
            MessageId messageid = message.getMessageId();
            String data = new String(message.getData());
            String key = message.getKey();
            System.out.println("receive " + topicName + " message messageId(" + messageid + ")" + ",value:" + data
                    + ",key:" + key);
            consumer.acknowledge(messageid);
        }
    }

    //测试Topic设置Property
    public static void sendMessageByProperty(PulsarClient client) throws PulsarClientException {
        String topic = "public/default/topic-property";
        // 构造消费者
        Consumer<byte[]> consumer = PulsarUtil.getPulsarConsumer(client, topic, "sub_property");
        // 构造生产者
        Producer<byte[]> producer = PulsarUtil.getPulsarProducer(client, topic);
        int numMessage = 10;
        for (int i = 0; i < numMessage; i++) {
            String propertyKey = "myproperty-key-" + i;
            String propertyValue = "myproperty-value-" + i;
            String value = "myvalue-" + i;
            producer.newMessage()
                    .property(propertyKey, propertyValue)
                    .value(value.getBytes())
                    .send();
        }

        for (int i = 0; i < numMessage; i++) {
            Message<byte[]> message = consumer.receive(3000, TimeUnit.MILLISECONDS);
            MessageId messageid = message.getMessageId();
            String data = new String(message.getData());
            System.out.println("receive message messageId(" + messageid + ")" + ",value:" + data
                    + ",property:" + message.getProperties());
            consumer.acknowledge(messageid);
        }
    }

    public static void sendMessageByBatch(PulsarClient client) throws Exception {
        String topic = "public/default/topic-batch";
        // 构造消费者
        Consumer<byte[]> consumer = PulsarUtil.getPulsarConsumer(client, topic, "sub_batch");

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .batchingMaxMessages(100)
                .batchingMaxBytes(1024 * 1024)
                .batcherBuilder(BatcherBuilder.KEY_BASED)
                .batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS)
                .blockIfQueueFull(true)
                .sendTimeout(30000, TimeUnit.MILLISECONDS)
                .create();

        int numMessage = 20;

        for (int i = 0; i < numMessage; i++) {
            String key = "mykey-" + i;
            producer.newMessage()
                    .key(key)
                    .value("batch message" + i)
                    .sendAsync().thenAccept(MessageId -> {
                        System.out.println("send batch message success, messageId(" + MessageId + ")");
                    }).exceptionally(e -> {
                        e.printStackTrace();
                        return null;
                    });
        }

        for (int i = 0; i < numMessage; i++) {
            Message<byte[]> message = consumer.receive(3000, TimeUnit.MILLISECONDS);
            String topicName = message.getTopicName();
            MessageId messageid = message.getMessageId();
            String data = new String(message.getData());
            String key = message.getKey();
            System.out.println("receive " + topicName + " message messageId(" + messageid + ")" + ",value:" + data
                    + ",key:" + key);
            consumer.acknowledge(messageid);
        }
    }


    public static void main(String[] args) throws Exception {
        PulsarClient client = PulsarUtil.getPulsarClient(Contents.SERVER_URL);
        // 设置key
        sendMessageByKey(client);
        // 设置Property
        sendMessageByProperty(client);
        // 设置batch
        sendMessageByBatch(client);
        client.close();
    }
}
