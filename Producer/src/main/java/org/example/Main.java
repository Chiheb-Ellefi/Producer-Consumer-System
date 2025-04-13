package org.example;

import org.example.comoponents.Client;
import org.example.data.WordGenerator;
import org.example.models.Message;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) {
        try {
            // Number of client threads to run in parallel
            int clientCount = 10;

            // Create a list to hold all client instances
            List<Client> clients = new ArrayList<>();
            WordGenerator wordGenerator = new WordGenerator();
            List<String> dictionary = wordGenerator.getData();
            String topic=wordGenerator.topic;

            // Create an ExecutorService with a fixed thread pool
            ExecutorService executorService = Executors.newFixedThreadPool(clientCount);

            // Create and start 10 client threads
            for (int i = 0; i < clientCount; i++) {
                Client client = new Client();
                clients.add(client);
                executorService.submit(client);
                System.out.println("Started client thread " + i);
            }

            AtomicInteger count = new AtomicInteger(0);
            // Send 100 messages to each client in parallel
            for (int msgId = 0; msgId < 100; msgId++) {
                final int messageId = msgId;
                // Create and send messages to all clients in parallel
                for (int clientId = 0; clientId < clientCount; clientId++) {
                    final int currentClientId = clientId;
                    final Client client = clients.get(clientId);
                    count.getAndIncrement();
                    if (count.get() >= dictionary.size()) {
                        System.err.println("Dictionary size exceeded: " + count.get() + " >= " + dictionary.size());
                        // Handle this case - perhaps cycle back to beginning
                        count.set(count.get() % dictionary.size());
                    }
                    if (client.queue.remainingCapacity() < 10) {
                        System.out.println("Warning: Client " + currentClientId + " queue is almost full");
                    }
                    // Submit a task to send a message to this client
                    executorService.submit(() -> {
                        try {
                            Message message = new Message();
                            message.setTopic(topic);
                            String content = dictionary.get(count.get());
                            message.setContent(content.getBytes(StandardCharsets.UTF_8));
                            // Log attempt before sending
                            System.out.println("Thread " + Thread.currentThread().getName() +
                                    " sending message " + messageId + " to client " + currentClientId);

                            // Queue the message for the client
                            client.queue.put(message);

                        } catch (InterruptedException e) {
                            System.err.println("Interrupted while sending message");
                            Thread.currentThread().interrupt();
                        }
                    });
                }

                // Small delay between batches to make output more readable
                Thread.sleep(100 );
            }

            // Wait for a while to allow processing
            System.out.println("Waiting for messages to be processed...");
            Thread.sleep(5000);

            // Shutdown the executor service
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    System.out.println("Forcing executor shutdown");
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                System.err.println("Executor shutdown interrupted");
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }

            System.out.println("Test completed");

        } catch (IOException | InterruptedException e) {
           System.err.println(e.getMessage());
        }
    }
}