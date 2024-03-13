package com.ascentstream.example.base;

import com.ascentstream.example.utils.Contents;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

public class HelloWorldConsumer {
    public static String topic = "persistent://public/default/test";

    public static void main(String[] args) {

        try {
            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(Contents.SERVER_URL)
                    .connectionTimeout(3000, TimeUnit.MILLISECONDS)
                    .authentication(
                            AuthenticationFactory.token(Contents.PRODUCE_CONSUMER_TOKEN)
                    ).build();

            Consumer<byte[]> consumer = client.newConsumer()
                    .topic(topic)
                    .subscriptionName("sub-0")
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();

            consumer.seek(consumer.getLastMessageId());
            System.out.println("receive start:");
            while (true) {
                Message<byte[]> message = consumer.receive(3, TimeUnit.SECONDS);
                if (message == null) {
                    break;
                }
                MessageId messageid = message.getMessageId();
                String data = new String(message.getData());
                System.out.println(" receive message messageId(" + messageid + ")" + ",value:" + data);
                consumer.acknowledge(messageid);
            }
            client.close();
            System.out.println("receive end");
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }
}
