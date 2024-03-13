package com.ascentstream.example.advance;

import com.ascentstream.example.utils.Contents;
import com.ascentstream.example.utils.PulsarUtil;
import com.ascentstream.example.utils.User;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.schema.JSONSchema;

public class SchemaDemo {

    public static void main(String[] args) throws Exception {
        PulsarClient client = PulsarUtil.getPulsarClient(Contents.SERVER_URL);
        String topic = "public/default/topic";
        Consumer<User> consumer = client.newConsumer(JSONSchema.of(User.class)).topic(topic)
                .subscriptionName("sub-schema").subscribe();
        Producer<User> producer = client.newProducer(JSONSchema.of(User.class))
                .topic(topic)
                .create();
        for (int i = 0; i < 10; i++) {
            User user = new User("Test" + i, i, 1, System.currentTimeMillis());
            System.out.println(user.getCreateTime());
            MessageId msgId = producer.newMessage()
                    .eventTime(PulsarUtil.TimeToStamp("2022-04-02 12:00:00:111"))
                    .value(user)
                    .send();
            System.out.println(msgId);
        }
        Message<User> message = consumer.receive();
        MessageId messageid = message.getMessageId();
        User data = message.getValue();
        String topicName = message.getTopicName();
        String key = message.getKey();
        System.out.println(
                "receive message " + topicName + " messageId(" + messageid + ")" + ",value:" + data + ",key:" + key);
        consumer.acknowledge(messageid);
        client.close();
    }
}