package internal.example.pubsub;

import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
@SuppressWarnings("checkstyle:abbreviationaswordinname")
public class QuickStartIT {

    private ByteArrayOutputStream bout;
    private String projectId = ServiceOptions.getDefaultProjectId();
    private String topicId = formatForTest("my-topic");
    private String subscriptionId = formatForTest("my-sub");

    private String formatForTest(String name) {
        return name + "-" + java.util.UUID.randomUUID().toString();
    }

    private void deleteTestTopics() {
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            topicAdminClient.deleteTopic(ProjectTopicName.of(projectId, topicId));
        } catch (IOException e) {
            System.err.println("Error deleting topic " + e.getMessage());
        }

    }

    private void deleteTestSubscription() {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create() ){
            subscriptionAdminClient.deleteSubscription(ProjectSubscriptionName.of(projectId, subscriptionId));
        } catch (IOException e) {
            System.err.println("Error deleting subscription " + e.getMessage());
        }

    }

    @Before
    public void setUp() {
        this.bout = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bout);
        System.setOut(out);
        try {
            deleteTestSubscription();
            deleteTestTopics();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        System.setOut(null);
        deleteTestSubscription();
        deleteTestTopics();
    }

    @Test
    public void testQuickstart() {
        // TODO implement this
        fail();
    }
}