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
import java.io.PrintStream;
import java.util.*;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
@SuppressWarnings("checkstyle:abbreviationaswordinname")
public class QuickStartIT {

    private ByteArrayOutputStream bout;
    private String projectId = ServiceOptions.getDefaultProjectId();
    private String topicId = formatForTest("my-topic");
    private String subscriptionId = formatForTest("my-sub");
    private int messageCount = 5;

    private String formatForTest(String name) {
        return name + "-" + java.util.UUID.randomUUID().toString();
    }

    private void deleteTestTopics() {
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            topicAdminClient.deleteTopic(ProjectTopicName.of(projectId, topicId));
        } catch (Exception e) {
            System.err.println("Error deleting topic " + e.getMessage());
        }

    }

    private void deleteTestSubscription() {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create() ){
            subscriptionAdminClient.deleteSubscription(ProjectSubscriptionName.of(projectId, subscriptionId));
        } catch (Exception e) {
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
    public void testQuickstart() throws Exception {
        // create a topic
        CreateTopicExample.main(topicId);
        String got = bout.toString();
        assertThat(got).contains(topicId + " created.");

        // create a subscriber
        CreatePullSubscriptionExample.main(topicId, subscriptionId);
        got = bout.toString();
        assertThat(got).contains(subscriptionId + " created.");

        bout.reset();

        // publish messages
        PublisherExample.main(topicId, String.valueOf(messageCount));
        String[] messageIds = bout.toString().split("\n");
        assertThat(messageIds).hasLength(messageCount);

        bout.reset();

        Thread subscriberThread = new Thread(new SubscriberRunnable(subscriptionId));
        subscriberThread.start();
        Set<String> expectedMessageIds = new HashSet<>();
        List<String> receivedMessageIds = new ArrayList<>();
        expectedMessageIds.addAll(Arrays.asList(messageIds));
        while (!expectedMessageIds.isEmpty()) {
            for(String expectedId : expectedMessageIds) {
                String tmp = bout.toString();
                if (tmp.contains(expectedId)) {
                    receivedMessageIds.add(expectedId);
                }
            }
            expectedMessageIds.removeAll(receivedMessageIds);
        }
        assertThat(expectedMessageIds).isEmpty();
    }

    private class SubscriberRunnable implements Runnable {

        private final String subscriptionId;

        public SubscriberRunnable(String subscriptionId) {
            this.subscriptionId = subscriptionId;
        }

        @Override
        public void run() {
            try {
                SubscriberExample.main(subscriptionId);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}