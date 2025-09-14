package com.timmax.training_demo.transaction_isolation_level;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Scanner;

import static com.timmax.training_demo.transaction_isolation_level.ConnectConstants.HOST;
import static com.timmax.training_demo.transaction_isolation_level.ConnectConstants.PORT;

public class SimpleWebSocketServer extends WebSocketServer {
    // Инициализация логгера
    private static final Logger logger = LoggerFactory.getLogger(SimpleWebSocketServer.class);

    public SimpleWebSocketServer() {
        // Привязка к хосту и порту
        super(new InetSocketAddress(HOST, PORT));
    }

    @Override
    public void start() {
        super.start();
        try {
            Thread.sleep(100);
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        logger.info("Новое соединение: {}", conn.getRemoteSocketAddress());
        conn.send("Добро пожаловать на WebSocket-сервер!");
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        logger.info("Клиент отключился: {}, причина: {}", conn.getRemoteSocketAddress(), reason);
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        // Callback: вызывается при получении данных
        logger.info("Получено сообщение от {}: {}", conn.getRemoteSocketAddress(), message);

        // Отправляем ответы (многократная отправка)
        conn.send("Сервер подтверждает: " + message);

        // Закрываем соединение, если получено "exit"
        if (message.equalsIgnoreCase("exit")) {
            logger.info("Клиент {} отправил 'exit', закрываем соединение", conn.getRemoteSocketAddress());
            conn.close();
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        logger.error("Ошибка: {}", ex.getMessage(), ex);
        if (conn != null) {
            conn.close();
        }
        if (!(ex instanceof BindException)) {
            logger.error("Не предвиденная ошибка! Требуется дополнительный анализ.");
        }
        System.exit(1);
    }

    @Override
    public void onStart() {
        logger.info("Сервер запущен на ws://{}", getAddress());
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("If you see message with incorrect symbols in log you can run 'chcp 65001' before running this application.");

        WebSocketServer server = new SimpleWebSocketServer();
        server.start();

        // Читаем ввод с консоли и выходим, если введено 'exit'
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                System.out.println("Введите 'exit' для выхода");
                String input = scanner.nextLine();
                if (input.equalsIgnoreCase("exit")) {
                    logger.info("Введено 'exit'. Сервер будет остановлен");
                    break;
                }
            }
        }
        server.stop();
        logger.info("Сервер остановлен на ws://{}", server.getAddress());
    }
}
