package com.timmax.training_demo.transaction_isolation_level.v02;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbSelectViaMainThreadTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbSelectViaMainThreadTest.class);


    //  Для этого и других ...ViaMainThread методов:
    //  Работает только потому, что select(where) объявлен как package-private и тесты находятся в том-же пакете.
    //  Прямое использование select(where) нежелательно, т.к. все SQL команды следует выполнять в транзакции.
    @Test
    public void selectFromEmptyTableViaMainThread() {
        //  SELECT *
        //    FROM person   --  0 rows
        DbSelect dbSelect = dbTabPersonEmpty.select(dbRec -> true).getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void selectFromOneRowTableViaMainThread() {
        //  SELECT *
        //    FROM person   --  1 row (dbRec1_Bob_email)
        DbSelect dbSelect = dbTabPersonWithOneRow.select(dbRec -> true).getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void selectFromTwoRowsTableViaMainThread() {
        //  SELECT *
        //    FROM person   --  2 rows (dbRec1_Bob_email, dbRec2_Alice_email)
        DbSelect dbSelect = dbTabPersonWithTwoRows.select(dbRec -> true).getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRows, dbSelect);
    }

    @Test
    public void selectFromEmptyTableWhereIdEq1ViaMainThread() {
        //  SELECT *
        //    FROM person   --  0 rows
        //   WHERE id = 1
        DbSelect dbSelect = dbTabPersonEmpty.select(
                dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(1)
        ).getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void selectFromOneRowTableWhereIdEq1ViaMainThread() {
        //  SELECT *
        //    FROM person   --  1 row (dbRec1_Bob_email)
        //   WHERE id = 1
        DbSelect dbSelect = dbTabPersonWithOneRow.select(
                dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(1)
        ).getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void selectFromTwoRowsTableWhereIdEq1ViaMainThread() {
        //  SELECT *
        //    FROM person   --  2 rows (dbRec1_Bob_email, dbRec2_Alice_email)
        //   WHERE id = 1
        DbSelect dbSelect = dbTabPersonWithTwoRows.select(
                dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(1)
        ).getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }
}
