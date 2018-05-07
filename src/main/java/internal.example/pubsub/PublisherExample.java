package internal.example.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

public class PublisherExample {
    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();

    public static void main(String... args) throws Exception {

        String topicId = args[0];
        int messageCount = Integer.parseInt(args[1]);

        ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, topicId);

        Publisher publisher = null;

        try {
            publisher = Publisher.newBuilder(topicName).build();

            for (int i = 0; i < messageCount; i++) {
                String message = "message-" + i;

                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                        .setData(data)
                        .build();
                ApiFuture<String> future = publisher.publish(pubsubMessage);

                ApiFutures.addCallback(future, new ApiFutureCallback<String>() {
                    @Override
                    public void onFailure(Throwable throwable) {
                        if (throwable instanceof ApiException) {
                            ApiException apiException = ((ApiException) throwable);
                            System.out.println(apiException.getStatusCode().getCode());
                            System.out.println(apiException.isRetryable());
                        }
                        System.out.println("Error publishing message : " + message);
                    }

                    @Override
                    public void onSuccess(String messageId) {
                        System.out.println(messageId);
                    }
                });
            }
        } finally {
            if (publisher != null) {
                publisher.shutdown();
            }
        }
    }
}
