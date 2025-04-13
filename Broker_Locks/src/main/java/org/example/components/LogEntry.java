package org.example.components;

import org.example.models.TopicMessage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.example.components.IndexEntry.findPositionInIndex;
import static org.example.components.IndexEntry.getNextOffset;

public class LogEntry {
    // A concurrent map to store locks for each topic, ensuring thread safety for log operations.
    public static final ConcurrentHashMap<String, ReentrantReadWriteLock> topicLocks = new ConcurrentHashMap<>();

    // Appends a message to the log file for the given topic.
    public static void appendMessage(String topic, byte[] message) throws IOException {
        ReentrantReadWriteLock lock = topicLocks.computeIfAbsent(topic, k -> new ReentrantReadWriteLock());
        lock.writeLock().lock(); // Acquire a write lock to ensure exclusive access.
        try (RandomAccessFile logFile = new RandomAccessFile("./logs/" + topic + ".dat", "rw")) {
            long position = logFile.length(); // Move to the end of the file.
            logFile.seek(position);

            long offset = getNextOffset(topic); // Get the next offset for the message.
            logFile.writeLong(offset); // Write the offset.
            logFile.writeInt(message.length); // Write the size of the message.
            logFile.write(message); // Write the actual message data.

            // Update index periodically (e.g., every 100 messages) to optimize lookups.
            IndexEntry.updateIndex(topic, offset, position);
        } finally {
            lock.writeLock().unlock(); // Release the write lock.
        }
    }

    // Reads messages from the log file starting from a given offset, up to maxCount messages.
    public static List<TopicMessage> readMessages(String topic, long startOffset, int maxCount) throws IOException {
        List<TopicMessage> messages = new ArrayList<>();
        ReentrantReadWriteLock lock = topicLocks.get(topic);
        if (lock == null) {
            return messages; // Return empty list if the topic doesn't exist.
        }

        lock.readLock().lock(); // Acquire a read lock to allow multiple readers.
        try (RandomAccessFile logFile = new RandomAccessFile("./logs/" + topic + ".dat", "r")) {
            long position = findPositionInIndex(topic, startOffset); // Find the starting position from the index.
            logFile.seek(position);
            int count = 0;

            while (count < maxCount && logFile.getFilePointer() < logFile.length()) {
                long offset = logFile.readLong(); // Read the offset.
                if (offset >= startOffset) {
                    int size = logFile.readInt(); // Read the size of the message.
                    byte[] payload = new byte[size];
                    logFile.read(payload); // Read the actual message data.
                    messages.add(new TopicMessage(topic, offset, payload)); // Store the message.
                    count++;
                } else {
                    // Skip this message if it's before the start offset.
                    int size = logFile.readInt();
                    logFile.skipBytes(size);
                }
            }
            return messages;
        } finally {
            lock.readLock().unlock(); // Release the read lock.
        }
    }
}
