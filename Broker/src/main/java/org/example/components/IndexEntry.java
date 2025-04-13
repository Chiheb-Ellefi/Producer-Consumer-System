package org.example.components;

import java.io.IOException;
import java.io.RandomAccessFile;

public class IndexEntry {
    // Index file stores (long offset, long position) pairs.

    public static long findPositionInIndex(String topic, long targetOffset) throws IOException {
        RandomAccessFile indexFile = new RandomAccessFile("./indexes/" + topic + ".dat", "r");

        // Binary search to locate the offset in the index file.
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
                // Found exact match, return position.
                long position = indexFile.readLong();
                indexFile.close();
                return position;
            }
        }

        // No exact match, return the closest lower offset's position if available.
        if (high >= 0) {
            indexFile.seek(high * 16);
            indexFile.readLong(); // Skip offset
            long position = indexFile.readLong();
            indexFile.close();
            return position;
        }

        // No lower offset found, return start position.
        indexFile.close();
        return 0;
    }

    public static void updateIndex(String topic, long offset, long position) throws IOException {
        if (offset % 100 == 0) { // Update index only for offsets that are multiples of 100.
            RandomAccessFile indexFile = new RandomAccessFile("./indexes/" + topic + ".dat", "rw");
            indexFile.seek(indexFile.length()); // Append new entry at the end.
            indexFile.writeLong(offset);
            indexFile.writeLong(position);
            indexFile.close();
        }
    }

    public static long getNextOffset(String topic) throws IOException {
        RandomAccessFile indexFile = new RandomAccessFile("./indexes/" + topic + ".dat", "rw");
        long length = indexFile.length();
        long position = 0;

        // Read the last offset and position from the index file.
        if (length > 0) {
            indexFile.seek(length - 16);
            indexFile.readLong(); // Read and discard the last offset.
            position = indexFile.readLong();
        }
        indexFile.close();

        RandomAccessFile logFile = new RandomAccessFile("./logs/" + topic + ".dat", "r");
        long currentOffset = 0;

        // Traverse the log file to find the next offset.
        logFile.seek(position);
        while (logFile.getFilePointer() < logFile.length()) {
            currentOffset = logFile.readLong(); // Read current offset.
            int size = logFile.readInt(); // Read message size.
            logFile.skipBytes(size); // Skip to next record.
        }

        logFile.close();
        return currentOffset + 1; // Return next available offset.
    }
}
