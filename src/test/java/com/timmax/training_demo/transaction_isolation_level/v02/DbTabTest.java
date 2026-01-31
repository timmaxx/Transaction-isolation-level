package com.timmax.training_demo.transaction_isolation_level.v02;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;
import static com.timmax.training_demo.transaction_isolation_level.v02.SQLCommandQueueUtil.startAllAndJoinToAllThreads;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DbTabTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbTabTest.class);

    SQLCommandQueue sqlCommandQueue;
    DbSelect dbSelect;


    @BeforeEach
    public void beforeEach() {
        sqlCommandQueue = new SQLCommandQueue();
    }


    @Test
    public void dbTabCopyMustHaveOwnDbRecCopy() {
        DbTab dbTabPerson = new DbTab(dbTabPersonRoWithOneRow, false);

        Assertions.assertNotSame(
                dbTabPersonRoWithOneRow.getRows().stream().findAny().get(),
                dbTabPerson.getRows().stream().findAny().get()
        );
    }

    @Test
    public void createTable() {
        //  CREATE TABLE person(
        //      id INT,
        //      name VARCHAR(50)
        //  )
        DbTab dbTabPerson = new DbTab(
                DB_TAB_NAME_PERSON,
                DB_FIELDS,
                true
        );
        assertEquals(0, dbTabPerson.count());

        //  SELECT *
        //    FROM person   --  0 rows
        sqlCommandQueue.add(
                dbTabPerson.getDQLCommandSelect()
        );
        startAllAndJoinToAllThreads(sqlCommandQueue);
        assertEquals(0, dbTabPerson.count());

        dbSelect = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void createTableAsCopyEmptyTableAndAddOneRow() {
        assertEquals(0, dbTabPersonRoEmpty.count());

        final DbTab dbTabPerson = new DbTab(dbTabPersonRoEmpty, true, List.of(dbRec1_Bob_email));

        assertEquals(1, dbTabPerson.count());

        //  SELECT *
        //    FROM person   --  1 row
        sqlCommandQueue.add(
                dbTabPerson.getDQLCommandSelect()
        );
        startAllAndJoinToAllThreads(sqlCommandQueue);

        dbSelect = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void createTableAsCopyOneRowTableAndAddOneRow() {
        assertEquals(1, dbTabPersonRoWithOneRow.count());

        final DbTab dbTabPerson = new DbTab(DbTestData.dbTabPersonRoWithOneRow, true, List.of(dbRec2_Alice_email));

        assertEquals(2, dbTabPerson.count());

        //  SELECT *
        //    FROM person   --  1 row
        sqlCommandQueue.add(
                dbTabPerson.getDQLCommandSelect()
        );
        startAllAndJoinToAllThreads(sqlCommandQueue);

        dbSelect = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRows, dbSelect);
    }

    @Test
    public void createTableAsCopyEmptyTableAndAddTwoRows() {
        assertEquals(0, dbTabPersonRoEmpty.count());

        final DbTab dbTabPerson = new DbTab(DbTestData.dbTabPersonRoEmpty, true, List.of(dbRec1_Bob_email, dbRec2_Alice_email));

        assertEquals(2, dbTabPerson.count());

        //  SELECT *
        //    FROM person   --  1 row
        sqlCommandQueue.add(
                dbTabPerson.getDQLCommandSelect()
        );
        startAllAndJoinToAllThreads(sqlCommandQueue);

        dbSelect = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRows, dbSelect);
    }
}
