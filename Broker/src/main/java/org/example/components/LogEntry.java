package org.example.components;

import org.example.models.TopicMessage;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static org.example.components.IndexEntry.findPositionInIndex;
import static org.example.components.IndexEntry.getNextOffset;

public class LogEntry {

    // Append a new message to the log file.
    public static void appendMessage(String topic, byte[] message) throws IOException {
        RandomAccessFile logFile = new RandomAccessFile("./logs/" + topic + ".dat", "rw");

        // Move to the end of the file to append the new message.
        long position = logFile.length();
        logFile.seek(position);

        // Get the next offset for the new message.
        long offset = getNextOffset(topic);

        // Write the message with a header (offset, size).
        logFile.writeLong(offset);
        logFile.writeInt(message.length);
        logFile.write(message);

        // Periodically update the index file.
        IndexEntry.updateIndex(topic, offset, position);
        logFile.close();
    }

    // Read messages from the log file starting from a given offset.
    public static List<TopicMessage> readMessages(String topic, long startOffset, int maxCount) throws IOException {
        List<TopicMessage> messages = new ArrayList<>();
        RandomAccessFile logFile = new RandomAccessFile("./logs/" + topic + ".dat", "r");

        // Find the starting position in the index file.
        long position = findPositionInIndex(topic, startOffset);
        logFile.seek(position);

        int count = 0;
        while (count < maxCount && logFile.getFilePointer() < logFile.length()) {
            long offset = logFile.readLong();

            if (offset > startOffset) {
                // Read the message size and content.
                int size = logFile.readInt();
                byte[] payload = new byte[size];
                logFile.read(payload);

                // Store the message in the result list.
                TopicMessage message = new TopicMessage(topic, offset, payload);
                messages.add(message);
                count++;
            } else {
                // Skip this message if it's before the start offset.
                int size = logFile.readInt();
                logFile.skipBytes(size);
            }
        }

        logFile.close();
        return messages;
    }
}
