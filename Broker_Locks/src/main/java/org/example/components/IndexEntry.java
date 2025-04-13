package org.example.components;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IndexEntry {
    // A concurrent map to store locks for each topic, ensuring thread safety in read/write operations.
    private final static ConcurrentHashMap<String, ReentrantReadWriteLock> topicLocks = new ConcurrentHashMap<>();

    // Searches for the position of the given offset in the index file of the specified topic.
    public static long findPositionInIndex(String topic, long targetOffset) throws IOException {
        ReentrantReadWriteLock lock = topicLocks.get(topic);
        if (lock == null) {
            return 0; // If no lock exists for the topic, return 0 (assumes no index exists).
        }

        lock.readLock().lock(); // Acquire a read lock to prevent writes while searching.
        try (RandomAccessFile indexFile = new RandomAccessFile("./indexes/" + topic + ".dat", "r")) {
            if (indexFile.length() == 0) {
                return 0; // Return 0 if the index file is empty.
            }

            // Binary search through the index file (each entry is 16 bytes: 8-byte offset + 8-byte position).
            long low = 0;
            long high = indexFile.length() / 16 - 1;

            while (low <= high) {
                long mid = (low + high) / 2;
                indexFile.seek(mid * 16); // Seek to the middle entry.
                long offset = indexFile.readLong();

                if (offset < targetOffset) {
                    low = mid + 1; // Search in the upper half.
                } else if (offset > targetOffset) {
                    high = mid - 1; // Search in the lower half.
                } else {
                    // Exact match found, return the associated position.
                    long position = indexFile.readLong();
                    return position;
                }
            }

            // If no exact match, return the position of the closest lower offset.
            if (high >= 0) {
                indexFile.seek(high * 16);
                indexFile.readLong(); // Skip offset
                long position = indexFile.readLong();
                return position;
            }

            // No valid lower offset found, return 0.
            return 0;
        } finally {
            lock.readLock().unlock(); // Release the read lock.
        }
    }

    // Updates the index file by appending a new offset-position pair (only for every 100th offset).
    public static void updateIndex(String topic, long offset, long position) throws IOException {
        if (offset % 100 == 0) { // Only update index at every 100th offset to reduce writes.
            ReentrantReadWriteLock lock = topicLocks.computeIfAbsent(topic, k -> new ReentrantReadWriteLock());
            lock.writeLock().lock(); // Acquire write lock to ensure exclusive access.
            try (RandomAccessFile indexFile = new RandomAccessFile("./indexes/" + topic + ".dat", "rw")) {
                indexFile.seek(indexFile.length()); // Move to the end of the file.
                indexFile.writeLong(offset); // Write the offset.
                indexFile.writeLong(position); // Write the corresponding position.
            } finally {
                lock.writeLock().unlock(); // Release the write lock.
            }
        }
    }

    // Determines the next offset by checking both the index and log files.
    public static long getNextOffset(String topic) throws IOException {
        ReentrantReadWriteLock lock = topicLocks.computeIfAbsent(topic, k -> new ReentrantReadWriteLock());

        lock.readLock().lock(); // Acquire a read lock to ensure consistent reads.
        long currentOffset = 0;
        long position = 0;

        try {
            // Check the index file for the last recorded offset and position.
            try (RandomAccessFile indexFile = new RandomAccessFile("./indexes/" + topic + ".dat", "r")) {
                long length = indexFile.length();

                if (length > 0) {
                    indexFile.seek(length - 16); // Move to the last index entry.
                    currentOffset = indexFile.readLong(); // Read the last offset.
                    position = indexFile.readLong(); // Read the corresponding position.
                }
            } catch (IOException e) {
                // If the index file doesn't exist, assume offset starts from 0.
                return 0;
            }

            // Check the log file to find the latest message beyond the indexed position.
            try (RandomAccessFile logFile = new RandomAccessFile("./logs/" + topic + ".dat", "r")) {
                logFile.seek(position); // Move to the last known position.
                while (logFile.getFilePointer() < logFile.length()) {
                    currentOffset = logFile.readLong(); // Read the next offset.
                    int size = logFile.readInt(); // Read the message size.
                    logFile.skipBytes(size); // Skip the message content.
                }
            } catch (IOException e) {
                // If the log file doesn't exist, return 0 or the last known offset.
                if (currentOffset == 0) {
                    return 0;
                }
            }

            return currentOffset + 1; // Return the next available offset.
        } finally {
            lock.readLock().unlock(); // Release the read lock.
        }
    }
}
