package org.example.comoponents;

import org.example.models.Message;

import java.io.*;
import java.net.Socket;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Client implements Runnable {
    private  Socket socket;
    private final String host;
    private final int port;
    private DataInputStream in;
    private DataOutputStream out;
    public BlockingQueue<Message> queue;
    public Client() throws IOException {
        this.host = "127.0.0.1";
        this.port = 8080;
        this.socket = new Socket(host,port);
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
        out.writeUTF("PRODUCER");
        this.queue=new LinkedBlockingQueue<>();
    }
    @Override
    public void run() {
        startMessageSender();

    }
    public void startMessageSender(){
        Thread messageSender=new Thread(()->{
            while(true){
                try {
               Message message=queue.take();
               sendMessage(message);
                } catch (IOException | InterruptedException e) {
                  System.out.println(e.getMessage());
                }
            }
        });
        messageSender.setDaemon(true);
        messageSender.start();
    }
    private void sendMessage(Message message) throws IOException, InterruptedException {
        out.writeUTF("topic");
        out.writeUTF(message.getTopic());
        out.writeInt(message.getSize());
        out.write(message.getContent());
        out.flush();
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



}




