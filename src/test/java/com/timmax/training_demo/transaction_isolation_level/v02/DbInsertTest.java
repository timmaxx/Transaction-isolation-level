package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.SQLCommandQueue;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandInsert;

import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql.DQLCommandSelect;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTab.*;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbInsertTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbInsertTest.class);

    @Test
    public void insertIntoReadOnlyTableViaMainThread() {
        //  INSERT
        //    INTO person   --  0 rows and table is read only
        //      (id, name, email)
        //    VALUES
        //      (1, "Bob", "@")
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                () -> dbTabPersonEmpty.insert(dbRec1_Bob_email)
        );

        Assertions.assertEquals(
                String.format(ERROR_TABLE_IS_RO_YOU_CANNOT_INSERT, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void insertIntoReadOnlyTableViaSQLCommandQueue() {
        //  INSERT
        //    INTO person   --  0 rows and table is read only
        //      (id, name, email)
        //    VALUES
        //      (1, "Bob", "@")
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DMLCommandInsert(dbTabPersonEmpty, dbRec1_Bob_email)
        );
        sqlCommandQueue1.startThread();
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                sqlCommandQueue1::joinToThread
        );

        Assertions.assertEquals(
                String.format(ERROR_TABLE_IS_RO_YOU_CANNOT_INSERT, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void insertOneRowWithEmailIsNullIntoEmptyTableViaMainThread() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name)
        //    VALUES
        //      (3, "Tom")
        dbTabPerson.insert(dbRec3_Tom_Null);

        DbSelect dbSelect = dbTabPerson.select().getDbSelect();

        Assertions.assertEquals(dbSelectPersonWithOneRowEmailIsNull, dbSelect);
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
                new DMLCommandInsert(dbTabPerson, dbRec3_Tom_Null),
                new DQLCommandSelect(dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonWithOneRowEmailIsNull, dbSelect);
    }

    @Test
    public void insertOneRowWithEmailIsNull2IntoEmptyTableViaMainThread() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name, email)
        //    VALUES
        //      (3, "Tom", null)
        dbTabPerson.insert(dbRec3_Tom_Null2);

        DbSelect dbSelect = dbTabPerson.select().getDbSelect();

        Assertions.assertEquals(dbSelectPersonWithOneRowEmailIsNull, dbSelect);
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
                new DMLCommandInsert(dbTabPerson, dbRec3_Tom_Null2),
                new DQLCommandSelect(dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonWithOneRowEmailIsNull, dbSelect);
    }

    @Test
    public void insertOneRowIntoEmptyTableViaMainThread() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name, email)
        //    VALUES
        //      (1, "Bob", "@")
        dbTabPerson.insert(dbRec1_Bob_email);

        DbSelect dbSelect = dbTabPerson.select().getDbSelect();

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    private void insertOneRowIntoEmptyTableViaSQLCommandQueue(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name, email)
        //    VALUES
        //      (1, "Bob", "@")
        sqlCommandQueue1.add(
                new DMLCommandInsert(dbTabPerson, dbRec1_Bob_email),
                new DQLCommandSelect(dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void insertOneRowIntoEmptyTableViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);
        SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertOneRowIntoEmptyTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);
    }

    @Test
    public void insertTwoRowsAtTimeIntoEmptyTableViaMainThread() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name, email)
        //    VALUES
        //      (1, "Bob", "@"),
        //      (2, "Alice", "@")
        dbTabPerson.insert(List.of(dbRec1_Bob_email, dbRec2_Alice_email));

        DbSelect dbSelect = dbTabPerson.select().getDbSelect();

        Assertions.assertEquals(dbSelectPersonWithTwoRows, dbSelect);
    }

    private void insertTwoRowsAtTimeIntoEmptyTableViaSQLCommandQueue(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  INSERT
        //    INTO person   --  0 rows
        //      (id, name, email)
        //    VALUES
        //      (1, "Bob", "@"),
        //      (2, "Alice", "@")
        sqlCommandQueue1.add(
                new DMLCommandInsert(dbTabPerson, List.of(dbRec1_Bob_email, dbRec2_Alice_email)),
                new DQLCommandSelect(dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonWithTwoRows, dbSelect);
    }

    @Test
    public void insertTwoRowsAtTimeIntoEmptyTableViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertTwoRowsAtTimeIntoEmptyTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);
    }

    @Test
    public void insertTwoRowsIntoEmptyTablesInDifferentOrderViaMainThread() {
        DbTab dbTabPerson1 = new DbTab(dbTabPersonEmpty, false);
        DbTab dbTabPerson2 = new DbTab(dbTabPersonEmpty, false);

        //  INSERT INTO person1 (id, name, email) VALUES (1, "Bob", "@");   --  0 rows
        //  INSERT INTO person1 (id, name, email) VALUES (2, "Alice", "@"); --  1 rows
        //  INSERT INTO person2 (id, name, email) VALUES (2, "Alice", "@"); --  0 rows
        //  INSERT INTO person2 (id, name, email) VALUES (1, "Bob", "@");   --  1 rows
        dbTabPerson1.insert(dbRec1_Bob_email);
        dbTabPerson1.insert(dbRec2_Alice_email);
        dbTabPerson2.insert(dbRec2_Alice_email);
        dbTabPerson2.insert(dbRec1_Bob_email);

        DbSelect dbSelect1 = dbTabPerson1.select().getDbSelect();
        DbSelect dbSelect2 = dbTabPerson2.select().getDbSelect();

        // Assertions.assertEquals(dbSelect1, dbSelect2);

        //  ToDo:   Нужно переделать. Т.к. сейчас в тесте вручную сортировать приходится.
        List<DbRec> values1 = new ArrayList<>(dbSelect1.getRows());
        List<DbRec> values2 = new ArrayList<>(dbSelect2.getRows());
        values1.sort(Comparator.naturalOrder());
        values2.sort(Comparator.naturalOrder());

        Assertions.assertEquals(values1, values2);
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
                new DMLCommandInsert(dbTabPerson1, dbRec1_Bob_email),
                new DMLCommandInsert(dbTabPerson1, dbRec2_Alice_email),
                new DMLCommandInsert(dbTabPerson2, dbRec2_Alice_email),
                new DMLCommandInsert(dbTabPerson2, dbRec1_Bob_email),
                new DQLCommandSelect(dbTabPerson1),
                new DQLCommandSelect(dbTabPerson2)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect2 = sqlCommandQueue1.popFromDQLResultLog();
        DbSelect dbSelect1 = sqlCommandQueue1.popFromDQLResultLog();

        // Assertions.assertEquals(dbSelect1, dbSelect2);

        //  ToDo:   Нужно переделать. Т.к. сейчас в тесте вручную сортировать приходится.
        List<DbRec> values1 = new ArrayList<>(dbSelect1.getRows());
        List<DbRec> values2 = new ArrayList<>(dbSelect2.getRows());
        values1.sort(Comparator.naturalOrder());
        values2.sort(Comparator.naturalOrder());

        Assertions.assertEquals(values1, values2);
    }

    //  AndRollBack
    @Test
    public void insertOneRowIntoEmptyTableViaSQLCommandQueueAndRollBack() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertOneRowIntoEmptyTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();
        //  Смущает, что селект после отката сделал не через SQLCommandQueue:
        DbSelect dbSelect2 = dbTabPerson.select().getDbSelect();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect2);
    }

    //  AndRollBack
    @Test
    public void insertTwoRowsAtTimeIntoEmptyTableViaSQLCommandQueueAndRollBack() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        insertTwoRowsAtTimeIntoEmptyTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();
        //  Смущает, что селект после отката сделал не через SQLCommandQueue:
        DbSelect dbSelect2 = dbTabPerson.select().getDbSelect();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect2);
    }
}
