package org.example.components;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class IndexEntry {
    // Map to store semaphores for each topic to handle concurrency
    private static final ConcurrentHashMap<String, Semaphore> topicSemaphores = new ConcurrentHashMap<>();

    public static long findPositionInIndex(String topic, long targetOffset) throws IOException, InterruptedException {
        Semaphore mutex = topicSemaphores.computeIfAbsent(topic, s -> new Semaphore(1));
        mutex.acquire(); // Ensure mutual exclusion for file access

        try (RandomAccessFile indexFile = new RandomAccessFile("./indexes/" + topic + ".dat", "r")) {
            // Perform binary search to locate the target offset
            long low = 0;
            long high = indexFile.length() / 16 - 1;

            while (low <= high) {
                long mid = (low + high) / 2;
                indexFile.seek(mid * 16);
                long offset = indexFile.readLong();

                if (offset < targetOffset) {
                    low = mid + 1;
                } else if (offset > targetOffset) {
                    high = mid - 1;
                } else {
                    // Exact match found, return position
                    return indexFile.readLong();
                }
            }

            // If no exact match, return the closest lower offset position
            if (high >= 0) {
                indexFile.seek(high * 16);
                indexFile.readLong(); // Skip offset
                return indexFile.readLong();
            }

            // Default to position 0 if no suitable offset is found
            return 0;
        } finally {
            mutex.release(); // Release the semaphore after file operations
        }
    }

    public static void updateIndex(String topic, long offset, long position) throws IOException, InterruptedException {
        if (offset % 100 == 0) { // Only update the index at specific intervals
            Semaphore mutex = topicSemaphores.computeIfAbsent(topic, s -> new Semaphore(1));
            mutex.acquire();

            try (RandomAccessFile indexFile = new RandomAccessFile("./indexes/" + topic + ".dat", "rw")) {
                indexFile.seek(indexFile.length()); // Append new entry
                indexFile.writeLong(offset);
                indexFile.writeLong(position);
            } finally {
                mutex.release();
            }
        }
    }

    public static long getNextOffset(String topic) throws IOException, InterruptedException {
        Semaphore mutex = topicSemaphores.computeIfAbsent(topic, s -> new Semaphore(1));
        mutex.acquire();
        long currentOffset = 0;
        long position = 0;

        try (RandomAccessFile indexFile = new RandomAccessFile("./indexes/" + topic + ".dat", "rw")) {
            long length = indexFile.length();

            if (length > 0) {
                indexFile.seek(length - 16); // Read last index entry
                currentOffset = indexFile.readLong();
                position = indexFile.readLong();
            }
        } finally {
            mutex.release();
        }

        try (RandomAccessFile logFile = new RandomAccessFile("./logs/" + topic + ".dat", "r")) {
            logFile.seek(position);

            // Traverse the log file to determine the next offset
            while (logFile.getFilePointer() < logFile.length()) {
                currentOffset = logFile.readLong();
                int size = logFile.readInt();
                logFile.skipBytes(size); // Skip over log entry content
            }
        }

        return currentOffset + 1; // Return the next available offset
    }
}
