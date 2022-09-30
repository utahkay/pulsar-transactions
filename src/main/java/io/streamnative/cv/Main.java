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
//    private static String pulsarCluster = "test-2-9";
//    private static String pulsarClusterNamespace = "cv-pulsar";
//    private static String pulsarUrl = "test-2-9.cv-pulsar.sn3.dev";
//    private static String credsFileName = "sa-continuous-verification-staging.json";

    private static String pulsarCluster = "test-2-8";
    private static String pulsarClusterNamespace = "cv-pulsar";
    private static String pulsarUrl = "test-2-8.cv-pulsar.sn3.dev";
    private static String credsFileName = "sa-continuous-verification-staging.json";

//    private static String pulsarCluster = "test-kj";
//    private static String pulsarClusterNamespace = "test-kay-johansen";
//    private static String pulsarUrl = "test-kj-49e27491-4df5-49ea-a239-f16b1ec6ba7b.gcp-shared-gcp-usce1-eagle.streamnative.g.sn3.dev";
//    private static String credsFileName = "test-kj-bot.json";

    private static String serviceUrl = String.format("pulsar+ssl://%s:6651", pulsarUrl);
    private static String oauthAudience = String.format("urn:sn:pulsar:%s:%s", pulsarClusterNamespace, pulsarCluster);
    private static String oauthIssuerUrl = "https://auth.sncloud-stg.dev/";
    private static String credentialsUrl = String.format("file:///Users/kayjohansen/service-account/%s", credsFileName);

    private static String topic = "test-transactions";
    private static String producerName = "kj-producer-1";
    private static boolean enableTransaction = true;

    public static void main(String[] args) throws MalformedURLException, PulsarClientException, ExecutionException, InterruptedException {
        new Main().run();
        System.out.println("Finished!");
    }

    private void run() throws MalformedURLException, PulsarClientException, ExecutionException, InterruptedException {
        PulsarClient client = null;
        Producer<String> producer = null;
        try {
            System.out.println("Transactions enabled: " + enableTransaction);
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
        String key = UUID.randomUUID().toString();
        Transaction transaction = null;
        if (enableTransaction) {
            transaction = client.newTransaction().withTransactionTimeout(5, TimeUnit.MINUTES).build().get();
            TypedMessageBuilder<String> mb = producer.newMessage(transaction).key(key);
            MessageId id = mb.value(msg).send();
            transaction.commit().get();
        } else {
            TypedMessageBuilder<String> mb = producer.newMessage().key(key);
            MessageId id = mb.value(msg).send();
        }
    }

    private Producer<String> createProducer(PulsarClient client, String producerName, String topic) throws PulsarClientException {
        Producer<String> producer = client
                .newProducer(Schema.STRING)
                .topic(topic)
                .producerName(producerName)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();
        return producer;
    }

    private PulsarClient getClient() throws MalformedURLException, PulsarClientException {
        ClientBuilder pulsarClientBuilder = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .enableTransaction(enableTransaction);

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
