package org.example.handlers;

import org.example.components.CustomLogger;
import org.example.components.LogEntry;
import java.io.*;
import java.net.Socket;



public class ProducerHandler implements Runnable {
    Socket socket;
    DataOutputStream out;
    DataInputStream in;
    CustomLogger logger;

    public ProducerHandler(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
        this.logger = new CustomLogger();
    }

    @Override
    public void run() {
        startMessageListener();

    }
    public void startMessageListener() {
        Thread listenerThread = new Thread(() -> {
            try {
                while (!socket.isClosed()) {
                    String command = in.readUTF();
                    if (command.equals("topic")) {
                        handleTopicMessage();
                    }
                }
            } catch (IOException | InterruptedException e) {
                logger.warning("End of stream reached. No more data to read.");
            }
        });

        listenerThread.setDaemon(true);
        listenerThread.start();
    }
    private void handleTopicMessage() throws IOException, InterruptedException {
        String topic = in.readUTF();
        int size = in.readInt();
        byte[] content = new byte[size];
        in.readFully(content);
        logger.info("Received message for topic: " + topic+ " Content: " + new String(content));
        LogEntry.appendMessage(topic, content);
    }


}
