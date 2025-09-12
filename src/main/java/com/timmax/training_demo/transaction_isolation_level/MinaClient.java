package com.timmax.training_demo.transaction_isolation_level;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class MinaClient {
    public static void main(String[] args) {
        // Создаём коннектор для клиента
        IoConnector connector = new NioSocketConnector();
        connector.getFilterChain().addLast("logger", new LoggingFilter());
        connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(
                new TextLineCodecFactory(StandardCharsets.UTF_8)));
        connector.setHandler(new ClientHandler());

        // Подключаемся к серверу
        ConnectFuture future = connector.connect(new InetSocketAddress("localhost", 8080));
        future.awaitUninterruptibly();
        IoSession session = future.getSession();
        System.out.println("Подключено к серверу. Введите сообщения (exit для выхода):");

        // Читаем ввод с консоли и отправляем
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                String input = scanner.nextLine();
                if (input.equalsIgnoreCase("exit")) {
                    break;
                }
                session.write(input);
            }
        }

        // Закрываем соединение
        System.out.println("Отключаемся от сервера");
        session.closeNow();
        connector.dispose();
    }

    // Обработчик событий клиента
    static class ClientHandler extends IoHandlerAdapter {
        @Override
        public void messageReceived(IoSession session, Object message) {
            // Callback: вызывается при получении ответа от сервера
            System.out.println("Ответ сервера: " + message);
        }

        @Override
        public void sessionOpened(IoSession session) {
            System.out.println("Соединение открыто");
        }

        @Override
        public void exceptionCaught(IoSession session, Throwable cause) {
            cause.printStackTrace();
            session.closeNow();
        }

        @Override
        public void sessionClosed(IoSession session) {
            System.out.println("Сервер отключился");
            System.exit(0);
        }
    }
}
