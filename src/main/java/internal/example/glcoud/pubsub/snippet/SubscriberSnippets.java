package internal.example.glcoud.pubsub.snippet;

import com.google.api.core.SettableApiFuture;
import com.google.cloud.pubsub.spi.v1.MessageReceiver;
import com.google.cloud.pubsub.spi.v1.Subscriber;
import com.google.pubsub.v1.SubscriptionName;

import java.util.concurrent.Executor;

/**
 * Methods for pub sub subscriber
 */
public class SubscriberSnippets {

    private final SubscriptionName subscriptionName;
    private final MessageReceiver receiver;
    private final SettableApiFuture<Void> done;
    private final Executor executor;

    public SubscriberSnippets(
            SubscriptionName subscriptionName,
            MessageReceiver messageReceiver,
            SettableApiFuture<Void> done,
            Executor executor) {
        this.subscriptionName = subscriptionName;
        this.receiver = messageReceiver;
        this.done = done;
        this.executor = executor;
    }

    public void startAndWait() throws Exception {
        Subscriber subscriber = Subscriber.defaultBuilder(subscriptionName, receiver).build();
        subscriber.addListener(new Subscriber.Listener() {
            public void failed(Subscriber.State from, Throwable failure) {
                // Handle error.
            }
        }, executor);
        subscriber.startAsync();

        done.get();
        subscriber.stopAsync().awaitTerminated();
    }
}
