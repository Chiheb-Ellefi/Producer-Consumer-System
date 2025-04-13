package org.example.server;

import org.example.handlers.ConsumerHandler;
import org.example.handlers.ProducerHandler;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
    private final int port;
    ServerSocket serverSocket;
    public Server(int port) throws IOException {
        this.port=port;

    }
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        System.out.println("Server started on port "+port);
        acceptConnections();
    }
    public void acceptConnections() throws IOException {
        while(!serverSocket.isClosed()){
            Socket clientSocket= serverSocket.accept();
            DataInputStream tempIn = new DataInputStream(clientSocket.getInputStream());
            String clientType = tempIn.readUTF();

            if (clientType.equals("PRODUCER")) {
                new Thread(new ProducerHandler(clientSocket)).start();
            } else if (clientType.equals("CONSUMER")) {
                new Thread(new ConsumerHandler(clientSocket)).start();
            } else {
                clientSocket.close();
            }
            System.out.println("Accepted connection from: "+clientSocket.getRemoteSocketAddress());

        }
    }

    public static void main(String[] args) {
     try{
         Server server = new Server(8080);
         server.start();

     } catch (IOException e) {
         System.out.println("Server failed to start");
     }
    }


}

