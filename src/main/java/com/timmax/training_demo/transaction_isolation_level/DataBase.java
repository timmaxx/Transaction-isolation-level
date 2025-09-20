package com.timmax.training_demo.transaction_isolation_level;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandInsert;
import com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataBase {
    private static final Logger logger = LoggerFactory.getLogger(DataBase.class);

    private final SomeTableInDB someTableInDB = new SomeTableInDB();
    private int sessionId = 0;

    public int createSession() {
        return sessionId++;
    }

    public static void main(String[] args) throws InterruptedException {
        DataBase dataBase = new DataBase();
        int session1 = dataBase.createSession();
        int session2 = dataBase.createSession();

        logger.info("1 (before initialize insert). dataBase.someTableInDB = {}", dataBase.someTableInDB);
        new SQLCommandInsert(
                session1,
                dataBase.someTableInDB,
                new SomeRecordInDB(123)).execute()
        ;
        Thread.sleep(200);

        logger.info("2 (before update in session 1). dataBase.someTableInDB = {}", dataBase.someTableInDB);
        new SQLCommandUpdate(
                session1,
                dataBase.someTableInDB).execute()
        ;

        logger.info("3 (before update in session 2). dataBase.someTableInDB = {}", dataBase.someTableInDB);
        new SQLCommandUpdate(
                session2,
                dataBase.someTableInDB).execute()
        ;

        Thread.sleep(2000);

        logger.info("4 (after all). dataBase.someTableInDB = {}", dataBase.someTableInDB);
    }
}
