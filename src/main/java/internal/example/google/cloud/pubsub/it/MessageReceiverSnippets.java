package internal.example.google.cloud.pubsub.it;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;

import java.util.concurrent.BlockingQueue;

/**
 * Methods for receiving pubsub messages
 */
public class MessageReceiverSnippets {
    private final BlockingQueue<PubsubMessage> blockingQueue;

    public MessageReceiverSnippets(BlockingQueue<PubsubMessage> blockingQueue) {
        this.blockingQueue = blockingQueue;
    }

    public MessageReceiver messageReceiver() {
        MessageReceiver receiver;
        receiver = new MessageReceiver() {
            @Override
            public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                if (blockingQueue.offer(message)) {
                    consumer.ack();
                } else {
                    consumer.nack();
                }
            }
        };
        return receiver;
    }
}
