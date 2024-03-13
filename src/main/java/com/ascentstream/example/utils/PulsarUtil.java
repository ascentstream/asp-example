package com.ascentstream.example.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

public class PulsarUtil {

    public static PulsarClient getPulsarClient(String serverUrl) throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(Contents.SERVER_URL)
                .connectionTimeout(30000, TimeUnit.MILLISECONDS)
                .authentication(
                        AuthenticationFactory.token(Contents.PRODUCE_CONSUMER_TOKEN)
                ).build();
    }

    public static Producer<byte[]> getPulsarProducer(PulsarClient client, String topic) throws PulsarClientException {
        return getPulsarProducer(client, topic, 30000);
    }

    public static Producer<byte[]> getPulsarProducer(PulsarClient client, String topic, int sendTimeout)
            throws PulsarClientException {
        return client.newProducer(Schema.BYTES)
                .topic(topic)
                .sendTimeout(sendTimeout, TimeUnit.MILLISECONDS)
                .create();
    }

    public static Consumer<byte[]> getPulsarConsumer(PulsarClient client, String topic, String subscriptionName)
            throws PulsarClientException {
        return client.newConsumer()
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscribe();
    }

    public static String getCurrentTime() {
        Date d = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(d);
    }

    public static String getTime(long timeStamp) {
        Date d = new Date(timeStamp);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(d);
    }

    public static long TimeToStamp(String date) throws Exception {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final Date datetime = sdf.parse(date);//将你的日期转换为时间戳
        return datetime.getTime();
    }
}
