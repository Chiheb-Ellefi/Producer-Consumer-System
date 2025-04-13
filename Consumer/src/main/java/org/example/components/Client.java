package org.example.components;



import org.example.models.TopicsHandler;

import java.io.*;
import java.net.Socket;
import java.util.Map;

public class Client implements Runnable {
    private Socket socket;
    private final String host;
    private final int port;
    private DataInputStream in;
    private DataOutputStream out;
    private final int MAX_COUNT=1000;
    private final TopicsHandler topicsHandler;
private Thread heartbeatThread;
    public Client() {
        this.host = "127.0.0.1";
        this.port = 8080;
        this.topicsHandler=new TopicsHandler();

    }
    public void subscribe(String topic) {
       topicsHandler.subscribe(topic);
    }

    @Override
    public void run() {
        try {
            setupConnection();
            startHeartbeat();
            startMessageListener();
            startMessageSender();
            while (true);
        } catch (IOException e) {
            System.err.println("Initial connection failed: " + e.getMessage());
            reconnect();
        }finally {
            cleanup();
        }
    }

    private void startHeartbeat() {
         heartbeatThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (out != null) {
                        out.writeUTF("*");
                        out.flush();  // Add this line
                    }
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Restore the interrupted status
                    break;
                } catch (Exception e) {
                    System.err.println("Heartbeat failed: " + e.getMessage());
                    reconnect();
                    break;
                }
            }
        });
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    private void reconnect() {
        while (true) {
            try {
                System.out.println("Attempting to reconnect...");
                stop();
                Thread.sleep(5000); // Wait before reconnecting
                setupConnection();
                startHeartbeat();
                startMessageListener();
                startMessageSender();
                System.out.println("Reconnected successfully!");
                return; // Exit the loop after successful reconnection
            } catch (InterruptedException e) {
                System.err.println("Reconnection interrupted: " + e.getMessage());
                Thread.currentThread().interrupt(); // Restore the interrupted status
                break;
            } catch (Exception e) {
                System.err.println("Reconnection failed: " + e.getMessage());
            }
        }
    }

    private void stop() throws IOException {
        try {
            if (in != null) in.close();
            if (out != null) out.close();
            if (socket != null) socket.close();
        } finally {
            in = null;
            out = null;
            socket = null;
        }
    }

    private void setupConnection() throws IOException {
        socket = new Socket(host, port);
        socket.setKeepAlive(true);
        socket.setSoTimeout(30000);
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
        out.writeUTF("CONSUMER");
    }
    private void cleanup() {
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
        }
        try {
            stop();
        } catch (IOException e) {
            System.err.println("Error during cleanup: " + e.getMessage());
        }
    }
    private void startMessageListener() {
        Thread messageListenerThread = new Thread(() -> {
            try {
                while (!socket.isClosed()) {
                    String message = in.readUTF(); // Use readUTF instead of readLine
                    switch (message) {
                        case "topic":
                            receiveMessage();
                            break;
                        case "quit":
                            break;
                        default:
                            System.out.println("Unknown message type: " + message);
                            break;
                    }
                }
            } catch (IOException e) {
                System.err.println("Connection error: " + e.getMessage());
                reconnect();
            }
        });
        messageListenerThread.setDaemon(true);
        messageListenerThread.start();
    }
  private void startMessageSender() {
        Thread messageSenderThread = new Thread(() -> {
           while (!socket.isClosed()) {
               for (Map.Entry<String, Long> entry : topicsHandler.getTopics().entrySet()) {
                   String k = entry.getKey();
                   Long v = entry.getValue();
                   System.out.println("Sending last offset : "+ v + " for topic: " + k);
                   try {
                       out.writeUTF("topic");
                       out.writeUTF(k);
                       out.writeUTF("offset");
                       out.writeLong(v);
                       out.writeUTF("count");
                       out.writeInt(MAX_COUNT);
                   } catch (IOException e) {
                       System.err.println("Error during send: " + e.getMessage());
                   }
               }
               try {
                   Thread.sleep(2000);
               } catch (InterruptedException e) {
                   System.err.println("Sleep interrupted: " + e.getMessage());
               }
           }
        });
        messageSenderThread.setDaemon(true);
        messageSenderThread.start();
  }
  private void receiveMessage() throws IOException{
      String topic=  in.readUTF();
      in.readUTF();
      Long offset=  in.readLong();
      boolean isEqual=topicsHandler.isEqual(offset,topic);
      in.readUTF();
    int size=in.readInt();
    in.readUTF();
    byte[] message=new byte[size];
    in.readFully(message);
      if(!isEqual){
          topicsHandler.updateOffset(topic,offset);
          System.out.println("Received: " + new String(message) + " for topic: " + topic + " offset: " + offset);
      }
  }
}