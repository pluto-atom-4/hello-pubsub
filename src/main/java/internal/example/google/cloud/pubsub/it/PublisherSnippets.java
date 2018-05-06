package internal.example.google.cloud.pubsub.it;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

/**
 * Methods for pub sub publisher
 */
public class PublisherSnippets {

    private final Publisher publisher;

    public PublisherSnippets(Publisher publisher) {
        this.publisher = publisher;
    }

    public ApiFuture<String> publish(String message) {
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        ApiFuture<String> messageIdFeature = publisher.publish(pubsubMessage);
        ApiFutures.addCallback(messageIdFeature, new ApiFutureCallback<String>() {
            @Override
            public void onSuccess(String messageId) {
                System.out.println("published with message id: " + messageId);

            }

            @Override
            public void onFailure(Throwable t) {
                System.out.println("failed to publish: " + t);
            }
        });
        return messageIdFeature;
    }
}
