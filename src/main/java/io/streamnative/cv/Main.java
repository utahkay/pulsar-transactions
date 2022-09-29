package io.streamnative.cv;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Main {
    private static String serviceUrl = "pulsar+ssl://test-kj-49e27491-4df5-49ea-a239-f16b1ec6ba7b.gcp-shared-gcp-usce1-eagle.streamnative.g.sn3.dev:6651";
    private static String oauthAudience = "urn:sn:pulsar:test-kay-johansen:test-kj";
    private static String oauthIssuerUrl = "https://auth.sncloud-stg.dev/";
    private static String credentialsUrl = "file:///Users/kayjohansen/service-account/test-kj-bot.json";
    private static String topic = "test-transactions";
    private static String producerName = "kj-producer-1";

    public static void main(String[] args) throws MalformedURLException, PulsarClientException, ExecutionException, InterruptedException {
        new Main().run();
        System.out.println("Finished!");
    }

    private void run() throws MalformedURLException, PulsarClientException, ExecutionException, InterruptedException {
        PulsarClient client = null;
        Producer<String> producer = null;
        try {
            System.out.println("Creating Pulsar client");
            client = getClient();
            System.out.println("Successfully created pulsar client");

            System.out.println(String.format("Creating producer %s for topic %s", producerName, topic));
            producer = createProducer(client, producerName, topic);
            System.out.println(String.format("Successfully created producer %s for topic %s", producerName, topic));

            String msg = "whats your favorite movie?";
            System.out.println("Producing a message");
            produceOneMessage(client, producer, msg);
            System.out.println("Produced one message");
        } finally {
            if (producer != null) producer.close();
            if (client != null) client.close();
        }
    }

    private void produceOneMessage(PulsarClient client, Producer<String> producer, String msg) throws PulsarClientException, ExecutionException, InterruptedException {
        Transaction transaction = client.newTransaction().withTransactionTimeout(5, TimeUnit.MINUTES).build().get();
        String key = UUID.randomUUID().toString();
        TypedMessageBuilder<String> mb = producer.newMessage(transaction).key(key);
        MessageId id = mb.value(msg).send();
        transaction.commit().get();
    }

    private Producer<String> createProducer(PulsarClient client, String producerName, String topic) throws PulsarClientException {
        Producer<String> producer = client
                .newProducer(Schema.STRING)
                .topic(topic)
                .producerName(producerName)
                .create();
        return producer;
    }

    private PulsarClient getClient() throws MalformedURLException, PulsarClientException {
        ClientBuilder pulsarClientBuilder = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .enableTransaction(true);

        final Authentication credentials = getOauthCredentials();
        if (credentials != null) {
            pulsarClientBuilder = pulsarClientBuilder.authentication(credentials);
        }

        return pulsarClientBuilder.build();
    }

    private Authentication getOauthCredentials() throws MalformedURLException {
        return AuthenticationFactoryOAuth2.clientCredentials(
                new URL(oauthIssuerUrl),
                new URL(credentialsUrl),
                oauthAudience);
    }
}
