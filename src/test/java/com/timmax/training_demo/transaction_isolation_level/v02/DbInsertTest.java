package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbRec.ERROR_COLUMN_DOESNT_EXIST;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbRec.ERROR_INVALID_INPUT_SYNTAX_FOR_COLUMN;
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
    public void insertOneRowWithNameIsNullIntoEmptyTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person (
        //      id
        //      ) VALUES (
        //      3
        //  )
        dbTabPerson.insert(dbRec3_Null);

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonWithOneRowNameIsNull, dbSelect);
    }

    @Test
    public void insertOneRowWithNameIsNull2IntoEmptyTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person (
        //      id, name
        //      ) VALUES (
        //      3, null
        //  )
        dbTabPerson.insert(dbRec3_Null2);

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonWithOneRowNameIsNull, dbSelect);
    }

    @Test
    public void insertOneRowWithIdAndNameAreNullIntoEmptyTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person
        //    VALUES (
        //      null, null
        //  )
        dbTabPerson.insert(dbRecNull_Null);

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonWithOneRowIdAndNameAreNull, dbSelect);
    }

    @Test
    public void insertTwoRowsWithIdAndNameAreNullIntoEmptyTable() {
        //  Этот тест таков потому, что в качестве коллекции для хранения строк используется Set.
        //  См. комментарий к DbTableLike :: dbRecs.
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT INTO person VALUES (null, null);
        //  INSERT INTO person VALUES (null, null);
        dbTabPerson.insert(dbRecNull_Null);
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbTabPerson.insert(dbRecNull_Null)
        );

        Assertions.assertEquals(
                String.format(ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void insertOneRowIntoEmptyTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person (
        //      id, name
        //      ) VALUES (
        //      1, "Bob"
        //  )
        dbTabPerson.insert(dbRec1_Bob);

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void insertTwoRowsIntoEmptyTableInDifferentOrder() {
        DbTab dbTabPerson1 = new DbTab(dbTabPersonEmpty, false);
        DbTab dbTabPerson2 = new DbTab(dbTabPersonEmpty, false);

        //  INSERT INTO person1 (id, name) VALUES (1, "Bob");
        //  INSERT INTO person1 (id, name) VALUES (2, "Alice");
        //  INSERT INTO person2 (id, name) VALUES (2, "Alice");
        //  INSERT INTO person2 (id, name) VALUES (1, "Bob");
        dbTabPerson1.insert(dbRec1_Bob);
        dbTabPerson1.insert(dbRec2_Alice);
        dbTabPerson2.insert(dbRec2_Alice);
        dbTabPerson2.insert(dbRec1_Bob);

        DbSelect dbSelect1 = dbTabPerson1.select();
        DbSelect dbSelect2 = dbTabPerson2.select();

        Assertions.assertEquals(dbSelect1, dbSelect2);
    }

    @Test
    public void insertOneRowIntoEmptyTableButFieldsHasWrongField() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person (
        //      wrong_name, name
        //      ) VALUES (
        //      1, "Bob"
        //  )
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbTabPerson.insert(
                        new DbRec(DB_FIELDS,
                                Map.of(DB_FIELD_NAME_WRONG_FIELD, 1,
                                        DB_FIELD_NAME_NAME, "Bob"
                                )
                        )
                )
        );

        Assertions.assertEquals(
                String.format("\n" +
                                ERROR_COLUMN_DOESNT_EXIST + "\n",
                        DB_FIELD_NAME_WRONG_FIELD
                ),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void insertOneRowIntoEmptyTableButValuesHasWrongTypeValues() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person (
        //      id, name
        //      ) VALUES (
        //      "B", 999
        //  )
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbTabPerson.insert(
                        new DbRec(DB_FIELDS,
                                Map.of(DB_FIELD_NAME_ID, "B",
                                        DB_FIELD_NAME_NAME, 999
                                )
                        )
                )
        );

        Assertions.assertEquals(
                String.format("\n" +
                                ERROR_INVALID_INPUT_SYNTAX_FOR_COLUMN + "\n" +
                                ERROR_INVALID_INPUT_SYNTAX_FOR_COLUMN + "\n",
                        Integer.class, DB_FIELD_NAME_ID, "B",
                        String.class, DB_FIELD_NAME_NAME, 999
                ),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }
}
