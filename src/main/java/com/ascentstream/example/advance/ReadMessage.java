package com.ascentstream.example.advance;

import com.ascentstream.example.utils.Contents;
import com.ascentstream.example.utils.PulsarUtil;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;

public class ReadMessage {


    public static void main(String[] args) throws PulsarClientException {
        PulsarClient client = PulsarUtil.getPulsarClient(Contents.SERVER_URL);
        String topic = "public/default/topic-read";
        Reader<byte[]> reader = client.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .create();
        Producer<byte[]> producer = PulsarUtil.getPulsarProducer(client, topic);
        int numMessage = 10;
        for (int i = 0; i < numMessage; i++) {
            MessageId messageId = producer.send(("sync message " + i).getBytes());
            System.out.println("send sync message success, messageId(" + messageId + ")");
        }

        while (true) {
            Message<byte[]> message = reader.readNext();
            if (message == null) {
                break;
            }
            MessageId messageid = message.getMessageId();
            String data = new String(message.getData());
            String topicName = message.getTopicName();
            System.out.println("receive " + topicName + " message messageId(" + messageid + ")" + ",value:" + data);
        }
        client.close();
    }
}
