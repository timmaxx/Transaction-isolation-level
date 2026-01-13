package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTab.ERROR_TABLE_IS_RO_YOU_CANNOT_INSERT;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DbInsertTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbInsertTest.class);

    DbTab dbTabPersonEmpty;
    SQLCommandQueue sqlCommandQueue;


    @BeforeEach
    public void beforeEach() {
        dbTabPersonEmpty = new DbTab(dbTabPersonRoEmpty, false);
        sqlCommandQueue = new SQLCommandQueue();
    }


    @Test
    public void insertIntoReadOnlyTable() {
        //  INSERT
        //    INTO person   --  0 rows and table is read only
        //      (id, name, email)
        //    VALUES
        //      (1, "Bob", "@")
        sqlCommandQueue.add(
                dbTabPersonRoEmpty.getDMLCommandInsert(dbRec1_Bob_email)
        );
        sqlCommandQueue.startThread();
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                sqlCommandQueue::joinToThread
        );

        assertEquals(
                String.format(ERROR_TABLE_IS_RO_YOU_CANNOT_INSERT, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void insertOneRowWithEmailIsNullIntoEmptyTable() {
        DbTab dbTabPerson = dbTabPersonEmpty;

        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name)
        //    VALUES
        //      (3, "Tom")
        sqlCommandQueue.add(
                dbTabPerson.getDMLCommandInsert(dbRec3_Tom_Null),
                dbTabPerson.getDQLCommandSelect()
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        DbSelect dbSelect = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRowEmailIsNull, dbSelect);
    }

    @Test
    public void insertOneRowWithEmailIsNull2IntoEmptyTable() {
        DbTab dbTabPerson = dbTabPersonEmpty;

        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name, email)
        //    VALUES
        //      (3, "Tom", null)
        sqlCommandQueue.add(
                dbTabPerson.getDMLCommandInsert(dbRec3_Tom_Null2),
                dbTabPerson.getDQLCommandSelect()
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        DbSelect dbSelect = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRowEmailIsNull, dbSelect);
    }

    private void insertOneRowIntoEmptyTable(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue) {
        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name, email)
        //    VALUES
        //      (1, "Bob", "@")
        sqlCommandQueue.add(
                dbTabPerson.getDMLCommandInsert(dbRec1_Bob_email),
                dbTabPerson.getDQLCommandSelect()
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        DbSelect dbSelect = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void insertOneRowIntoEmptyTable() {
        DbTab dbTabPerson = dbTabPersonEmpty;

        insertOneRowIntoEmptyTable(dbTabPerson, sqlCommandQueue);
    }

    private void insertTwoRowsAtTimeIntoEmptyTable(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue) {
        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name, email)
        //    VALUES
        //      (1, "Bob", "@"),
        //      (2, "Alice", "@")
        sqlCommandQueue.add(
                dbTabPerson.getDMLCommandInsert(List.of(dbRec1_Bob_email, dbRec2_Alice_email)),
                dbTabPerson.getDQLCommandSelect()
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        DbSelect dbSelect = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRows, dbSelect);
    }

    @Test
    public void insertTwoRowsAtTimeIntoEmptyTable() {
        DbTab dbTabPerson = dbTabPersonEmpty;

        insertTwoRowsAtTimeIntoEmptyTable(dbTabPerson, sqlCommandQueue);
    }

    @Test
    public void insertTwoRowsIntoEmptyTablesInDifferentOrder() {
        DbTab dbTabPerson1 = dbTabPersonEmpty;
        DbTab dbTabPerson2 = dbTabPersonEmpty;

        //  INSERT INTO person1 (id, name, email) VALUES (1, "Bob", "@");   --  0 rows
        //  INSERT INTO person1 (id, name, email) VALUES (2, "Alice", "@"); --  1 rows
        //  INSERT INTO person2 (id, name, email) VALUES (2, "Alice", "@"); --  0 rows
        //  INSERT INTO person2 (id, name, email) VALUES (1, "Bob", "@");   --  1 rows
        sqlCommandQueue.add(
                dbTabPerson1.getDMLCommandInsert(dbRec1_Bob_email),
                dbTabPerson1.getDMLCommandInsert(dbRec2_Alice_email),
                dbTabPerson2.getDMLCommandInsert(dbRec2_Alice_email),
                dbTabPerson2.getDMLCommandInsert(dbRec1_Bob_email),
                dbTabPerson1.getDQLCommandSelect(),
                dbTabPerson2.getDQLCommandSelect()
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        DbSelect dbSelect2 = sqlCommandQueue.popFromDQLResultLog();
        DbSelect dbSelect1 = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelect1, dbSelect2);
    }

    @Test
    public void insertOneRowIntoEmptyTableAndRollback() {
        DbTab dbTabPerson = dbTabPersonEmpty;

        insertOneRowIntoEmptyTable(dbTabPerson, sqlCommandQueue);

        //  ROLLBACK;
        sqlCommandQueue.rollback();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonEmpty, sqlCommandQueue, dbTabPerson);
    }

    @Test
    public void insertOneRowIntoEmptyTableAndCommit() {
        DbTab dbTabPerson = dbTabPersonEmpty;

        insertOneRowIntoEmptyTable(dbTabPerson, sqlCommandQueue);

        //  COMMIT;
        sqlCommandQueue.commit();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithOneRow, sqlCommandQueue, dbTabPerson);
    }

    @Test
    public void insertTwoRowsAtTimeIntoEmptyTableAndRollback() {
        DbTab dbTabPerson = dbTabPersonEmpty;

        insertTwoRowsAtTimeIntoEmptyTable(dbTabPerson, sqlCommandQueue);

        //  ROLLBACK;
        sqlCommandQueue.rollback();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonEmpty, sqlCommandQueue, dbTabPerson);
    }

    @Test
    public void insertTwoRowsAtTimeIntoEmptyTableAndCommit() {
        DbTab dbTabPerson = dbTabPersonEmpty;

        insertTwoRowsAtTimeIntoEmptyTable(dbTabPerson, sqlCommandQueue);

        //  COMMIT;
        sqlCommandQueue.commit();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRows, sqlCommandQueue, dbTabPerson);
    }
}
