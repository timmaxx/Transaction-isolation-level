package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.SQLCommandQueue;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbSelectTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbSelectTest.class);

    @Test
    public void selectFromEmptyTableViaMainThread() {
        //  SELECT *
        //    FROM person   --  0 rows
        DbSelect dbSelect = dbTabPersonEmpty.select().getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void selectFromEmptyTableViaSQLCommandQueue() {
        //  SELECT *
        //    FROM person   --  0 rows
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPersonEmpty.getDQLCommandSelect(dbTabPersonEmpty)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void selectFromOneRowTableViaMainThread() {
        //  SELECT *
        //    FROM person   --  1 row (dbRec1_Bob_email)
        DbSelect dbSelect = dbTabPersonWithOneRow.select().getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void selectFromOneRowTableViaSQLCommandQueue() {
        //  SELECT *
        //    FROM person   --  1 row (dbRec1_Bob_email)
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPersonWithOneRow.getDQLCommandSelect(dbTabPersonWithOneRow)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void selectFromTwoRowsTableViaMainThread() {
        //  SELECT *
        //    FROM person   --  2 rows (dbRec1_Bob_email, dbRec2_Alice_email)
        DbSelect dbSelect = dbTabPersonWithTwoRows.select().getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRows, dbSelect);
    }

    @Test
    public void selectFromTwoRowsTableViaSQLCommandQueue() {
        //  SELECT *
        //    FROM person   --  2 rows (dbRec1_Bob_email, dbRec2_Alice_email)
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPersonWithTwoRows.getDQLCommandSelect(dbTabPersonWithTwoRows)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

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
    public void selectFromEmptyTableWhereIdEq1ViaSQLCommandQueue() {
        //  SELECT *
        //    FROM person   --  0 rows
        //   WHERE id = 1
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPersonEmpty.getDQLCommandSelect(dbTabPersonEmpty, dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(1))
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

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
    public void selectFromOneRowTableWhereIdEq1ViaSQLCommandQueue() {
        //  SELECT *
        //    FROM person   --  1 row (dbRec1_Bob_email)
        //   WHERE id = 1
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPersonWithOneRow.getDQLCommandSelect(dbTabPersonWithOneRow, dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(1))
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

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

    @Test
    public void selectFromTwoRowsTableWhereIdEq1ViaSQLCommandQueue() {
        //  SELECT *
        //    FROM person   --  2 rows (dbRec1_Bob_email, dbRec2_Alice_email)
        //   WHERE id = 1
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPersonWithOneRow.getDQLCommandSelect(dbTabPersonWithTwoRows, dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(1))
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }
}
