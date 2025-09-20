package com.timmax.training_demo.transaction_isolation_level.websocket;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Scanner;

import static com.timmax.training_demo.transaction_isolation_level.websocket.ConnectConstants.SERVER_URL;

public class SimpleWebSocketClient extends WebSocketClient {
    private static final Logger logger = LoggerFactory.getLogger(SimpleWebSocketClient.class);

    public SimpleWebSocketClient(URI serverUri) {
        super(serverUri);
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        logger.info("{}. Подключено к серверу: {}", getLocalSocketAddress(), getURI());
    }

    @Override
    public void onMessage(String message) {
        // Callback: вызывается при получении данных от сервера
        logger.info("{}. Ответ сервера: {}", getLocalSocketAddress(), message);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        logger.info("{}. Соединение закрыто. Код: {}, причина: {}", getLocalSocketAddress(), code, reason);
        System.exit(0);
    }

    @Override
    public void onError(Exception ex) {
        logger.error("{}. Ошибка: {}", getLocalSocketAddress(), ex.getMessage());
        System.exit(1);
    }

    public static void main(String[] args) {
        try {
            // Создаём клиент и подключаемся к серверу
            SimpleWebSocketClient client = new SimpleWebSocketClient(new URI(SERVER_URL));
            client.connectBlocking(); // Блокируем, пока не подключимся

            // Читаем ввод с консоли и отправляем сообщение
            System.out.println("Введите сообщение (exit для выхода):");
            try (Scanner scanner = new Scanner(System.in)) {
                while (client.isOpen()) {
                    String message = scanner.nextLine().trim();
                    if (message.isEmpty()) {
                        continue;
                    }
                    client.send(message);
                    logger.info("{}. Отправлено: {}", client.getLocalSocketAddress(), message);
                    if (message.equalsIgnoreCase("exit")) {
                        client.close();
                        break;
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
