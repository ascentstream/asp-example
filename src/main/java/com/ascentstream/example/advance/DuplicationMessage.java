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

/*
 * 消息去重，需要Broker端开启去重配置.
 * */
public class DuplicationMessage {

    public static void main(String[] args) throws PulsarClientException {
        PulsarClient client = PulsarUtil.getPulsarClient(Contents.SERVER_URL);
        String topic = "public/default/topic-duplication";
        Consumer<byte[]> consumer = PulsarUtil.getPulsarConsumer(client, topic, "sub_duplication");
        Producer<byte[]> producer = PulsarUtil.getPulsarProducer(client, topic);
        int numMessages = 10;
        for (int i = 0; i < numMessages; i++) {
            MessageId messageId = producer.newMessage()
                    .sequenceId(i)
                    .value(("sync message " + i).getBytes())
                    .send();
            System.out.println("send sync message success, messageId(" + messageId + ")");
        }
        producer.newMessage()
                .sequenceId(9)
                .value(("sync message " + 9).getBytes())
                .send();
        while (true) {
            Message<byte[]> message = consumer.receive(3, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            MessageId messageId = message.getMessageId();
            String data = new String(message.getData());
            String topicName = message.getTopicName();
            System.out.println("receive " + topicName + " message messageId(" + messageId + ")" + ",value:" + data);
            consumer.acknowledge(messageId);
        }
        client.close();
    }
}
