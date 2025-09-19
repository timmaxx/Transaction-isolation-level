package com.timmax.training_demo.transaction_isolation_level;

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

        logger.info("1. dataBase.someTableInDB = {}", dataBase.someTableInDB);
        dataBase.someTableInDB.insert(session1, new SomeRecordInDB(123));
        Thread.sleep(200);
/*
        logger.info("2. dataBase.someTableInDB = {}", dataBase.someTableInDB);
        dataBase.someTableInDB.commit(session1);
*/
        logger.info("3. dataBase.someTableInDB = {}", dataBase.someTableInDB);
        dataBase.someTableInDB.updateSetField1EqualToField1Plus111(session1, 1);

        logger.info("4. dataBase.someTableInDB = {}", dataBase.someTableInDB);
        dataBase.someTableInDB.updateSetField1EqualToField1Plus111(session2, 1);
/*
        logger.info("5. dataBase.someTableInDB = {}", dataBase.someTableInDB);
        dataBase.someTableInDB.commit(session1);
        logger.info("6. dataBase.someTableInDB = {}", dataBase.someTableInDB);
        dataBase.someTableInDB.commit(session2);
*/
        Thread.sleep(10000);

        logger.info("7. dataBase.someTableInDB = {}", dataBase.someTableInDB);
    }
}
