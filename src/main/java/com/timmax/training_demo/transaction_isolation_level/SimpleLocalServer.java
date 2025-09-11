package com.timmax.training_demo.transaction_isolation_level;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class SimpleLocalServer {
    public static void main(String[] args) {
        try {
            // Bind the server to localhost only (127.0.0.1)
            InetAddress localHost = InetAddress.getByName("127.0.0.1");
            ServerSocket serverSocket = new ServerSocket(8080, 0, localHost);
            System.out.println("Server running on localhost:8080");

            while (true) {
                // Waiting for the client to connect
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected: " + clientSocket.getInetAddress());

                // Reading a message from a client
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String message = in.readLine();
                System.out.println("Received: " + message);

                // Sending a reply
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                out.println("Greetings from the server! Received: " + message);

                // Closing the connection
                clientSocket.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
