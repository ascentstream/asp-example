package com.ascentstream.example.base;

import com.ascentstream.example.utils.Contents;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

public class HelloWorldProducer {
    public static String topic = "persistent://public/default/test"; //根据Token对应的telnet和namespace

    public static void main(String[] args) {
        try {
            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(Contents.SERVER_URL)
                    .connectionTimeout(3000, TimeUnit.MILLISECONDS)
                    .authentication(
                            AuthenticationFactory.token(Contents.PRODUCE_CONSUMER_TOKEN)
                    ).build();
            // 构造生产者
            Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(topic)
                    .compressionType(CompressionType.LZ4)
                    .batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS)
                    .maxPendingMessages(1000)
                    .sendTimeout(30000, TimeUnit.MILLISECONDS)
                    .create();
            // 同步发送消息
            System.out.println("send start");
            for (int i = 0; i < 10; i++) {
                MessageId messageId = producer.send("sync message " + i);
                System.out.println("send sync message success, messageId(" + messageId + ")");
            }
            System.out.println("-----------------------------------------------");
            // 异步发送消息
            for (int i = 0; i < 10; i++) {
                producer.sendAsync("Hello" + i).thenAccept(MessageId -> {
                    System.out.println("send async message success, messageId(" + MessageId + ")");
                }).exceptionally(e -> {
                    e.printStackTrace();
                    return null;
                });
            }
            Thread.sleep(3000);
            client.close();
            System.out.println("send end");
        } catch (PulsarClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
