package internal.example.pubsub;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.ProjectTopicName;


public class CreateTopicExample {
    public static void main(String... args) throws Exception {

        String projectId = ServiceOptions.getDefaultProjectId();

        String topicId = args[0];

        ProjectTopicName topic = ProjectTopicName.of(projectId, topicId);
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()){
            topicAdminClient.createTopic(topic);
        } catch (ApiException e) {
            System.out.println(e.getStatusCode().getCode());
            System.out.println(e.isRetryable());
        }

        System.out.printf("Topic %s:%s created.\n", topic.getProject(), topic.getTopic());
    }
}
