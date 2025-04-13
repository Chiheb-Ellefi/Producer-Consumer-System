package org.example.handlers;



import org.example.components.CustomLogger;
import org.example.components.LogEntry;
import org.example.models.TopicMessage;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class ConsumerHandler implements Runnable {
    Socket socket;
    DataOutputStream out;
    DataInputStream in;
    BlockingQueue<TopicMessage> queue;
    CustomLogger logger;

    public ConsumerHandler(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
        this.queue = new LinkedBlockingQueue<>();
        this.logger = new CustomLogger();
    }

    @Override
    public void run() {
        startMessageListener();
        startMessageSender();
    }

    public void startMessageListener() {
        Thread listenerThread = new Thread(() -> {
            try {
                while (!socket.isClosed()) {
                    String command = in.readUTF();
                    switch (command) {
                        case "topic":
                            handleTopicMessage();
                            break;
                        case "*":
                            logger.info("Heartbeat received");
                            break;
                        default:
                            logger.info("Unknown command: " + command);
                            break;
                    }
                }
            } catch (IOException | InterruptedException e) {
                logger.warning("Connection terminated: " + e.getMessage());
                // Connection closed - handle cleanup if needed
            }
        });

        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    private void handleTopicMessage() throws IOException, InterruptedException {
        try {
            String topic = in.readUTF();
            String offsetTag = in.readUTF(); // Should be "offset"

            if (!offsetTag.equals("offset")) {
                logger.info("Protocol error: Expected 'offset', got '" + offsetTag + "'");
                return;
            }

            long offset =  in.readLong();
    String countTag = in.readUTF();
    if (!countTag.equals("count")) {
        logger.info("Protocol error: Expected 'count', got '" + countTag + "'");
        return;
    }
    int count = in.readInt();
            logger.info("Sending messages from topic " + topic + " from offset " + offset);
            List<TopicMessage> messages = LogEntry.readMessages(topic, offset,count);
            logger.info("Found " + messages.size() + " messages to send");

            for (TopicMessage message : messages) {
                queue.put(message);
            }
        } catch (Exception e) {
            logger.warning("Error in handleTopicMessage: " + e.getMessage());
            throw e; // Re-throw to be handled by caller
        }
    }

    public void startMessageSender() {
        Thread senderThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    TopicMessage message = queue.take();
                    sendMessage(message);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (IOException e) {
                    logger.warning("Error sending message: " + e.getMessage());
                    break;
                }
            }
        });
        senderThread.setDaemon(true);
        senderThread.start();
    }

    private void sendMessage(TopicMessage message) throws IOException {
        try {
            out.writeUTF("topic");
            out.writeUTF(message.getTopic());
            out.writeUTF("offset");
            out.writeLong(message.getOffset());
            out.writeUTF("size");
            out.writeInt(message.getPayload().length);
            out.writeUTF("payload");
            out.write(message.getPayload());
            out.flush();

            logger.info("Sent message for topic " + message.getTopic() + " with offset " + message.getOffset());
        } catch (Exception e) {
            logger.warning("Error in sendMessage: " + e.getMessage());
            throw e;
        }
    }
}