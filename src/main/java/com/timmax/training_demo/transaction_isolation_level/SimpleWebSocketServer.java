package com.timmax.training_demo.transaction_isolation_level;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.util.Scanner;

public class SimpleWebSocketServer extends WebSocketServer {
    public SimpleWebSocketServer() {
        super(new InetSocketAddress("localhost", 8080)); // Привязка к localhost:8080
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        System.out.println("Новое соединение: " + conn.getRemoteSocketAddress());
        conn.send("Добро пожаловать на WebSocket-сервер!");
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        System.out.println("Клиент отключился: " + conn.getRemoteSocketAddress() + ", причина: " + reason);
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        // Callback: вызывается при получении данных
        System.out.println("Получено сообщение от " + conn.getRemoteSocketAddress() + ": " + message);

        // Отправляем ответы (многократная отправка)
        conn.send("Сервер подтверждает: " + message);

        // Закрываем соединение, если получено "exit"
        if (message.equalsIgnoreCase("exit")) {
            conn.close();
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("Ошибка: " + ex.getMessage());
        if (conn != null) {
            conn.close();
        }
    }

    @Override
    public void onStart() {
        System.out.println("Сервер запущен на ws://localhost:8080");
    }

    public static void main(String[] args) throws InterruptedException {
        WebSocketServer server = new SimpleWebSocketServer();
        server.start();

        // Читаем ввод с консоли и выходим, если введено exit
        System.out.println("Введите exit для выхода");
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                String input = scanner.nextLine();
                if (input.equalsIgnoreCase("exit")) {
                    break;
                }
            }
        }
        server.stop();
    }
}
