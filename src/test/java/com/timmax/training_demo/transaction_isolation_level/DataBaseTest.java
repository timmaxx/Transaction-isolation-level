package com.timmax.training_demo.transaction_isolation_level;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommand;
import com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandInsert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.timmax.training_demo.transaction_isolation_level.TestData.afterOneRecordWasInserted;
import static com.timmax.training_demo.transaction_isolation_level.TestData.recordForOneInsert;

public class DataBaseTest {
    private static final Logger logger = LoggerFactory.getLogger(DataBaseTest.class);

    private final SomeTableInDB someTableInDB = new SomeTableInDB();

    @Test
    public void insertOneRecordIntoEmptyTable() {
        synchronized (this) {
            logger.debug("Main thread 1. Before insert");
            logger.debug("  dataBase.someTableInDB = {}", someTableInDB);
        }

        SQLCommand sqlCommand = new SQLCommandInsert(
                someTableInDB,
                recordForOneInsert
        );
        sqlCommand.startThread();

        synchronized (this) {
            logger.debug("Main thread 2. Just after calling insert.execute, but might be before fact executing");
            logger.debug("  dataBase.someTableInDB = {}", someTableInDB);
        }

        sqlCommand.joinToThread();

        synchronized (this) {
            logger.debug("Main thread 3. After all");
            logger.debug("  dataBase.someTableInDB = {}", someTableInDB);
        }

        Assertions.assertEquals(afterOneRecordWasInserted, someTableInDB);
    }

/*
    public static void main(String[] args) throws InterruptedException {
        DataBaseTest dataBaseTest = new DataBaseTest();
        int session1 = dataBaseTest.createSession();
        int session2 = dataBaseTest.createSession();

        logger.info("1 (before initialize insert). dataBase.someTableInDB = {}", dataBaseTest.someTableInDB);
        new SQLCommandInsert(
                session1,
                dataBaseTest.someTableInDB,
                new SomeRecordInDB(123)).execute()
        ;
        Thread.sleep(200);

        logger.info("2 (before update in session 1). dataBase.someTableInDB = {}", dataBaseTest.someTableInDB);
        new SQLCommandUpdate(
                session1,
                dataBaseTest.someTableInDB).execute()
        ;

        logger.info("3 (before update in session 2). dataBase.someTableInDB = {}", dataBaseTest.someTableInDB);
        new SQLCommandUpdate(
                session2,
                dataBaseTest.someTableInDB).execute()
        ;

        Thread.sleep(2000);

        logger.info("4 (after all). dataBase.someTableInDB = {}", dataBaseTest.someTableInDB);
    }
*/
}
