package com.timmax.training_demo.transaction_isolation_level;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class SimpleClient {
    public static void main(String[] args) {
        try {
            Socket socket = new Socket("127.0.0.1", 8080);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println("Hello, server!");

            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            System.out.println("Server response: " + in.readLine());

            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}