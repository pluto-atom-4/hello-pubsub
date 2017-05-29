package internal.example.glcoud.pubsub.snippet;

import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.ChannelProvider;
import com.google.cloud.NoCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.spi.v1.*;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

public class ITPubSubSnippets {

    private static final String NAME_SUFFIX = UUID.randomUUID().toString();

    private ChannelProvider providerForTopic = TopicAdminSettings.defaultChannelProviderBuilder()
            .setEndpoint(System.getenv("PUBSUB_EMULATOR_HOST"))
            .setCredentialsProvider(FixedCredentialsProvider.create(NoCredentials.getInstance()))
            .build();

    private ChannelProvider providerForSubscriber = SubscriptionAdminSettings.defaultChannelProviderBuilder()
            .setEndpoint(System.getenv("PUBSUB_EMULATOR_HOST"))
            .setCredentialsProvider(FixedCredentialsProvider.create(NoCredentials.getInstance()))
            .build();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(300);

    private static String formatForTest(String resourceName) {
        return resourceName + "-" + NAME_SUFFIX;
    }

    @Test
    public void testPublisherSubscriber() throws Exception {
        TopicName topicName = TopicName
                .create(ServiceOptions.getDefaultProjectId(), formatForTest("test-topic"));
        SubscriptionName subscriptionName = SubscriptionName
                .create(ServiceOptions.getDefaultProjectId(), formatForTest("test-subscription"));

        try (TopicAdminClient publisherClient = TopicAdminClient.create(TopicAdminSettings.defaultBuilder().setChannelProvider(providerForTopic).build());
             SubscriptionAdminClient subscriberClient = SubscriptionAdminClient.create(SubscriptionAdminSettings.defaultBuilder().setChannelProvider(providerForSubscriber).build())) {
            publisherClient.createTopic(topicName);
            subscriberClient.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 0);

            TestPublisherSubscriberHelper(topicName, subscriptionName);
            subscriberClient.deleteSubscription(subscriptionName);
            publisherClient.deleteTopic(topicName);
        }


    }

    private void TestPublisherSubscriberHelper(TopicName topicName, SubscriptionName subscriptionName) throws Exception {
        String messageToPublish = "my-message";

        Publisher publisher = null;
        try {
            publisher = Publisher.defaultBuilder(topicName).setChannelProvider(providerForTopic).build();
            PublisherSnippets snippets = new PublisherSnippets(publisher);
            final SettableApiFuture<Void> done = SettableApiFuture.create();
            ApiFutures.addCallback(
                    snippets.publish(messageToPublish),
                    new ApiFutureCallback<String>() {
                        @Override
                        public void onSuccess(String messageId) {
                            done.set(null);
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            done.setException(throwable);
                        }
                    }
            );
            done.get();
        } finally {
            if (publisher != null) {
                publisher.shutdown();
            }
        }

        final BlockingQueue<PubsubMessage> queue = new ArrayBlockingQueue<>(1);
        final SettableApiFuture<Void> done = SettableApiFuture.create();
        final SettableApiFuture<PubsubMessage> received = SettableApiFuture.create();
        SubscriberSnippets snippets =
                new SubscriberSnippets(
                        subscriptionName,
                        new MessageReceiverSnippets(queue).messageReceiver(),
                        done,
                        MoreExecutors.directExecutor()
                );
        new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            received.set(queue.poll(10, TimeUnit.MINUTES));
                        } catch (InterruptedException e) {
                            received.set(null);
                        }
                        done.set(null);
                    }
                }
        ).start();
        snippets.startAndWait();

        PubsubMessage  message = received.get();
        assertNotNull(message);
        assertEquals(message.getData().toStringUtf8(), messageToPublish);
    }

}
