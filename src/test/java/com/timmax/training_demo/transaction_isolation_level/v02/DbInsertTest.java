package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTab.ERROR_TABLE_IS_RO_YOU_CANNOT_INSERT;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DbInsertTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbInsertTest.class);


    @Test
    public void insertIntoReadOnlyTableViaSQLCommandQueue() {
        //  INSERT
        //    INTO person   --  0 rows and table is read only
        //      (id, name, email)
        //    VALUES
        //      (1, "Bob", "@")
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPersonEmpty.getDMLCommandInsert(dbTabPersonEmpty, dbRec1_Bob_email)
        );
        sqlCommandQueue1.startThread();
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                sqlCommandQueue1::joinToThread
        );

        assertEquals(
                String.format(ERROR_TABLE_IS_RO_YOU_CANNOT_INSERT, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void insertOneRowWithEmailIsNullIntoEmptyTableViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name)
        //    VALUES
        //      (3, "Tom")
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPerson.getDMLCommandInsert(dbTabPerson, dbRec3_Tom_Null),
                dbTabPerson.getDQLCommandSelect(dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRowEmailIsNull, dbSelect);
    }

    @Test
    public void insertOneRowWithEmailIsNull2IntoEmptyTableViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name, email)
        //    VALUES
        //      (3, "Tom", null)
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPerson.getDMLCommandInsert(dbTabPerson, dbRec3_Tom_Null2),
                dbTabPerson.getDQLCommandSelect(dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRowEmailIsNull, dbSelect);
    }

    private void insertOneRowIntoEmptyTableViaSQLCommandQueue(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name, email)
        //    VALUES
        //      (1, "Bob", "@")
        sqlCommandQueue1.add(
                dbTabPerson.getDMLCommandInsert(dbTabPerson, dbRec1_Bob_email),
                dbTabPerson.getDQLCommandSelect(dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void insertOneRowIntoEmptyTableViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);
        SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertOneRowIntoEmptyTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);
    }

    private void insertTwoRowsAtTimeIntoEmptyTableViaSQLCommandQueue(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name, email)
        //    VALUES
        //      (1, "Bob", "@"),
        //      (2, "Alice", "@")
        sqlCommandQueue1.add(
                dbTabPerson.getDMLCommandInsert(dbTabPerson, List.of(dbRec1_Bob_email, dbRec2_Alice_email)),
                dbTabPerson.getDQLCommandSelect(dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRows, dbSelect);
    }

    @Test
    public void insertTwoRowsAtTimeIntoEmptyTableViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertTwoRowsAtTimeIntoEmptyTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);
    }

    @Test
    public void insertTwoRowsIntoEmptyTablesInDifferentOrderViaSQLCommandQueue() {
        DbTab dbTabPerson1 = new DbTab(dbTabPersonEmpty, false);
        DbTab dbTabPerson2 = new DbTab(dbTabPersonEmpty, false);

        //  INSERT INTO person1 (id, name, email) VALUES (1, "Bob", "@");   --  0 rows
        //  INSERT INTO person1 (id, name, email) VALUES (2, "Alice", "@"); --  1 rows
        //  INSERT INTO person2 (id, name, email) VALUES (2, "Alice", "@"); --  0 rows
        //  INSERT INTO person2 (id, name, email) VALUES (1, "Bob", "@");   --  1 rows
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPerson1.getDMLCommandInsert(dbTabPerson1, dbRec1_Bob_email),
                dbTabPerson1.getDMLCommandInsert(dbTabPerson1, dbRec2_Alice_email),
                dbTabPerson2.getDMLCommandInsert(dbTabPerson2, dbRec2_Alice_email),
                dbTabPerson2.getDMLCommandInsert(dbTabPerson2, dbRec1_Bob_email),
                dbTabPerson1.getDQLCommandSelect(dbTabPerson1),
                dbTabPerson2.getDQLCommandSelect(dbTabPerson2)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect2 = sqlCommandQueue1.popFromDQLResultLog();
        DbSelect dbSelect1 = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelect1, dbSelect2);
    }

    @Test
    public void insertOneRowIntoEmptyTableViaSQLCommandQueueAndRollback() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertOneRowIntoEmptyTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();

        DbSelectUtil.selectFromDbTabViaSQLCommandQueueAndAssertEqualsWithExpectedDbSelect(dbSelectPersonEmpty, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void insertOneRowIntoEmptyTableViaSQLCommandQueueAndCommit() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertOneRowIntoEmptyTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  COMMIT;
        sqlCommandQueue1.commit();

        DbSelectUtil.selectFromDbTabViaSQLCommandQueueAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithOneRow, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void insertTwoRowsAtTimeIntoEmptyTableViaSQLCommandQueueAndRollback() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertTwoRowsAtTimeIntoEmptyTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();

        DbSelectUtil.selectFromDbTabViaSQLCommandQueueAndAssertEqualsWithExpectedDbSelect(dbSelectPersonEmpty, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void insertTwoRowsAtTimeIntoEmptyTableViaSQLCommandQueueAndCommit() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertTwoRowsAtTimeIntoEmptyTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  COMMIT;
        sqlCommandQueue1.commit();

        DbSelectUtil.selectFromDbTabViaSQLCommandQueueAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRows, sqlCommandQueue1, dbTabPerson);
    }
}
