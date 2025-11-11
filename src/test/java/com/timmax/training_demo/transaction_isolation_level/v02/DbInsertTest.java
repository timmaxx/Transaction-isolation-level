package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTab.*;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbInsertTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbInsertTest.class);

    @Test
    public void insertIntoReadOnlyTable() {
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                () -> dbTabPersonEmpty.insert(null)
        );

        Assertions.assertEquals(
                String.format(ERROR_TABLE_IS_RO_YOU_CANNOT_INSERT, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void insertOneRowWithEmailIsNullIntoEmptyTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person (
        //      id, name
        //      ) VALUES (
        //      3, "Tom"
        //  )
        dbTabPerson.insert(dbRec3_Tom_Null);

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonWithOneRowEmailIsNull, dbSelect);
    }

    @Test
    public void insertOneRowWithEmailIsNull2IntoEmptyTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person (
        //      id, name, email
        //      ) VALUES (
        //      3, "Tom", null
        //  )
        dbTabPerson.insert(dbRec3_Tom_Null2);

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonWithOneRowEmailIsNull, dbSelect);
    }

    @Test
    public void insertOneRowIntoEmptyTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person (
        //      id, name, email
        //      ) VALUES (
        //      1, "Bob", "@"
        //  )
        dbTabPerson.insert(dbRec1_Bob_email);

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    //  Этот тест нужен и таков потому, что в качестве коллекции для хранения строк используется Set.
    //  См. комментарий к DbTableLike :: dbRecs.
    @Test
    public void insertTwoRowsWithIdAndNameAreNullIntoEmptyTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT INTO person VALUES (1, "Bob", "@");
        //  INSERT INTO person VALUES (1, "Bob", "@");
        dbTabPerson.insert(dbRec1_Bob_email);
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbTabPerson.insert(dbRec1_Bob_email)
        );

        Assertions.assertEquals(
                String.format(ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void insertTwoRowsIntoEmptyTableInDifferentOrder() {
        DbTab dbTabPerson1 = new DbTab(dbTabPersonEmpty, false);
        DbTab dbTabPerson2 = new DbTab(dbTabPersonEmpty, false);

        //  INSERT INTO person1 (id, name, email) VALUES (1, "Bob", "@"););
        //  INSERT INTO person1 (id, name, email) VALUES (2, "Alice", "@");
        //  INSERT INTO person2 (id, name, email) VALUES (2, "Alice", "@");
        //  INSERT INTO person2 (id, name, email) VALUES (1, "Bob", "@"););
        dbTabPerson1.insert(dbRec1_Bob_email);
        dbTabPerson1.insert(dbRec2_Alice_email);
        dbTabPerson2.insert(dbRec2_Alice_email);
        dbTabPerson2.insert(dbRec1_Bob_email);

        DbSelect dbSelect1 = dbTabPerson1.select();
        DbSelect dbSelect2 = dbTabPerson2.select();

        Assertions.assertEquals(dbSelect1, dbSelect2);
    }

    //  ToDo:   Дополнить тестом, вставляющим запись с NOT NULL полем, которое = null.
}
