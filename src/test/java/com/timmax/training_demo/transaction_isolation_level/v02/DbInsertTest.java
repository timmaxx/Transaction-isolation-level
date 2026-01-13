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
    public void insertIntoReadOnlyTable() {
        //  INSERT
        //    INTO person   --  0 rows and table is read only
        //      (id, name, email)
        //    VALUES
        //      (1, "Bob", "@")
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPersonRoEmpty.getDMLCommandInsert(dbRec1_Bob_email)
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
    public void insertOneRowWithEmailIsNullIntoEmptyTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonRoEmpty, false);

        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name)
        //    VALUES
        //      (3, "Tom")
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPerson.getDMLCommandInsert(dbRec3_Tom_Null),
                dbTabPerson.getDQLCommandSelect()
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRowEmailIsNull, dbSelect);
    }

    @Test
    public void insertOneRowWithEmailIsNull2IntoEmptyTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonRoEmpty, false);

        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name, email)
        //    VALUES
        //      (3, "Tom", null)
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPerson.getDMLCommandInsert(dbRec3_Tom_Null2),
                dbTabPerson.getDQLCommandSelect()
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRowEmailIsNull, dbSelect);
    }

    private void insertOneRowIntoEmptyTable(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name, email)
        //    VALUES
        //      (1, "Bob", "@")
        sqlCommandQueue1.add(
                dbTabPerson.getDMLCommandInsert(dbRec1_Bob_email),
                dbTabPerson.getDQLCommandSelect()
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void insertOneRowIntoEmptyTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonRoEmpty, false);
        SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertOneRowIntoEmptyTable(dbTabPerson, sqlCommandQueue1);
    }

    private void insertTwoRowsAtTimeIntoEmptyTable(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name, email)
        //    VALUES
        //      (1, "Bob", "@"),
        //      (2, "Alice", "@")
        sqlCommandQueue1.add(
                dbTabPerson.getDMLCommandInsert(List.of(dbRec1_Bob_email, dbRec2_Alice_email)),
                dbTabPerson.getDQLCommandSelect()
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRows, dbSelect);
    }

    @Test
    public void insertTwoRowsAtTimeIntoEmptyTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonRoEmpty, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertTwoRowsAtTimeIntoEmptyTable(dbTabPerson, sqlCommandQueue1);
    }

    @Test
    public void insertTwoRowsIntoEmptyTablesInDifferentOrder() {
        DbTab dbTabPerson1 = new DbTab(dbTabPersonRoEmpty, false);
        DbTab dbTabPerson2 = new DbTab(dbTabPersonRoEmpty, false);

        //  INSERT INTO person1 (id, name, email) VALUES (1, "Bob", "@");   --  0 rows
        //  INSERT INTO person1 (id, name, email) VALUES (2, "Alice", "@"); --  1 rows
        //  INSERT INTO person2 (id, name, email) VALUES (2, "Alice", "@"); --  0 rows
        //  INSERT INTO person2 (id, name, email) VALUES (1, "Bob", "@");   --  1 rows
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPerson1.getDMLCommandInsert(dbRec1_Bob_email),
                dbTabPerson1.getDMLCommandInsert(dbRec2_Alice_email),
                dbTabPerson2.getDMLCommandInsert(dbRec2_Alice_email),
                dbTabPerson2.getDMLCommandInsert(dbRec1_Bob_email),
                dbTabPerson1.getDQLCommandSelect(),
                dbTabPerson2.getDQLCommandSelect()
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect2 = sqlCommandQueue1.popFromDQLResultLog();
        DbSelect dbSelect1 = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelect1, dbSelect2);
    }

    @Test
    public void insertOneRowIntoEmptyTableAndRollback() {
        DbTab dbTabPerson = new DbTab(dbTabPersonRoEmpty, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertOneRowIntoEmptyTable(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonEmpty, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void insertOneRowIntoEmptyTableAndCommit() {
        DbTab dbTabPerson = new DbTab(dbTabPersonRoEmpty, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertOneRowIntoEmptyTable(dbTabPerson, sqlCommandQueue1);

        //  COMMIT;
        sqlCommandQueue1.commit();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithOneRow, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void insertTwoRowsAtTimeIntoEmptyTableAndRollback() {
        DbTab dbTabPerson = new DbTab(dbTabPersonRoEmpty, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertTwoRowsAtTimeIntoEmptyTable(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonEmpty, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void insertTwoRowsAtTimeIntoEmptyTableAndCommit() {
        DbTab dbTabPerson = new DbTab(dbTabPersonRoEmpty, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertTwoRowsAtTimeIntoEmptyTable(dbTabPerson, sqlCommandQueue1);

        //  COMMIT;
        sqlCommandQueue1.commit();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRows, sqlCommandQueue1, dbTabPerson);
    }
}
