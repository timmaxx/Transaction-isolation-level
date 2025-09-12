package com.timmax.training_demo.transaction_isolation_level;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class MinaServer {
    public static void main(String[] args) throws Exception {
        // Создаём акцептор для TCP-соединений
        IoAcceptor acceptor = new NioSocketAcceptor();

        // Добавляем фильтры: логирование и кодек для текстовых строк
        acceptor.getFilterChain().addLast("logger", new LoggingFilter());
        acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(
                new TextLineCodecFactory(StandardCharsets.UTF_8)));

        // Устанавливаем обработчик событий
        ServerHandler serverHandler = new ServerHandler();
        acceptor.setHandler(serverHandler);

        // Настраиваем параметры сессии
        acceptor.getSessionConfig().setReadBufferSize(2048);
        acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);

        // Привязываем сервер к localhost:8080
        acceptor.bind(new InetSocketAddress("localhost", 8080));
        System.out.println("Сервер запущен на localhost:8080");

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

        // Закрываем соединение
        acceptor.dispose();
    }

    // Обработчик событий соединения
    static class ServerHandler extends IoHandlerAdapter {
        @Override
        public void sessionCreated(IoSession session) {
            System.out.println("Создано соединение с клиентом: " + session.getRemoteAddress());
        }

        @Override
        public void messageReceived(IoSession session, Object message) {
            // Callback: вызывается при получении данных
            String data = message.toString();
            System.out.println("Получены данные от клиента " + session.getRemoteAddress() + ": " + data);

            // Пример обработки: отправляем ответ
            session.write("Сервер подтверждает: " + data);
        }

        @Override
        public void sessionClosed(IoSession session) {
            System.out.println("Клиент " + session.getRemoteAddress() + " отключился");
        }

        @Override
        public void exceptionCaught(IoSession session, Throwable cause) {
            cause.printStackTrace();
            session.closeNow();
        }
    }
}
