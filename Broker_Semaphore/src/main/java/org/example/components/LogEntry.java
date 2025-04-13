package org.example.components;

import org.example.models.TopicMessage;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static org.example.components.IndexEntry.getNextOffset;

public class LogEntry {
    // Map to store semaphores for each topic to ensure mutual exclusion during file operations
    private static final ConcurrentHashMap<String, Semaphore> topicSemaphores = new ConcurrentHashMap<>();

    /**
     * Appends a new message to the log file for the given topic.
     * Uses a semaphore to ensure exclusive access to the log file during write operations.
     */
    public static void appendMessage(String topic, byte[] message) throws IOException, InterruptedException {
        Semaphore mutex = topicSemaphores.computeIfAbsent(topic, s -> new Semaphore(1));
        mutex.acquire(); // Acquire lock before writing to the file
        try (RandomAccessFile logFile = new RandomAccessFile("./logs/" + topic + ".dat", "rw")) {
            long position = logFile.length(); // Move to end of file
            logFile.seek(position);

            long offset = getNextOffset(topic); // Get the next offset for the message
            logFile.writeLong(offset);
            logFile.writeInt(message.length);
            logFile.write(message);

            // Update index to keep track of message positions
            IndexEntry.updateIndex(topic, offset, position);
        } finally {
            mutex.release(); // Release lock after writing
        }
    }

    /**
     * Reads messages from the log file starting from a given offset.
     * Ensures thread safety using a semaphore to prevent concurrent read inconsistencies.
     */
    public static List<TopicMessage> readMessages(String topic, long startOffset, int maxCount) throws IOException, InterruptedException {
        List<TopicMessage> messages = new ArrayList<>();
        Semaphore mutex = topicSemaphores.get(topic);
        if (mutex == null) {
            return messages; // No messages for this topic yet
        }
        mutex.acquire(); // Acquire lock before reading from the file
        try (RandomAccessFile logFile = new RandomAccessFile("./logs/" + topic + ".dat", "r")) {
            long position = IndexEntry.findPositionInIndex(topic, startOffset);
            logFile.seek(position);
            int count = 0;
            while (count < maxCount && logFile.getFilePointer() < logFile.length()) {
                long offset = logFile.readLong();
                if (offset > startOffset) { // Read only messages after the given offset
                    int size = logFile.readInt();
                    byte[] payload = new byte[size];
                    logFile.read(payload);
                    TopicMessage message = new TopicMessage(topic, offset, payload);
                    messages.add(message);
                    count++;
                } else {
                    int size = logFile.readInt(); // Skip messages before the startOffset
                    logFile.skipBytes(size);
                }
            }
        } finally {
            mutex.release(); // Release lock after reading
        }
        return messages;
    }
}
