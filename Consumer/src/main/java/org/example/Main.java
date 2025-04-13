package org.example;

import org.example.components.Client;

import java.util.Arrays;
import java.util.List;


public class Main {
    public static void main(String[] args) {
        Client client = new Client();
        List<String> topics= Arrays.asList("technology", "sports", "animals", "countries", "food", "music", "science", "geography");
        topics.forEach(client::subscribe);
        Thread thread = new Thread(client);
        thread.start();
    }
}