package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbFieldValue.ERROR_VALUE_IS_WRONG_TYPE;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbRec.ERROR_COLUMN_DOESNT_EXIST;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTab.*;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbDeleteTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbDeleteTest.class);

    @Test
    public void deleteFromReadOnlyTable() {
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                () -> dbTabPersonEmpty.delete(null)
        );
        Assertions.assertEquals(
                String.format(ERROR_TABLE_IS_RO_YOU_CANNOT_DELETE, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void deleteFromEmptyTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  DELETE
        //    FROM person
        dbTabPerson.delete();

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromOneRowTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithOneRow, false);

        //  DELETE
        //    FROM person
        dbTabPerson.delete();

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  DELETE
        //    FROM person
        //   WHERE id = 2
        dbTabPerson.delete(
                dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(2)
        );

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void deleteFromTwoRowsTableWhereHasWrongNameField() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  DELETE
        //    FROM person
        //   WHERE wrong_field = 2
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbTabPerson.delete(
                        dbRec -> dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD).equals(2)
                )
        );

        Assertions.assertEquals(
                String.format(ERROR_COLUMN_DOESNT_EXIST, DB_FIELD_NAME_WRONG_FIELD),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void deleteFromTwoRowsTableWhereHasWrongNameFields() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  DELETE
        //    FROM person
        //   WHERE wrong_field = 2
        //      or wrong_field_2 = 'Bob'
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                //  См. комментарии к DbUpdateTest :: updateTwoRowsTableButSetHasWrongFieldsAndWhereHasWrongNameFields
                () -> dbTabPerson.delete(
                        dbRec ->
                                dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD).equals(2) ||
                                        dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD_2).equals("Bob")
                )
        );

        Assertions.assertEquals(
                String.format(ERROR_COLUMN_DOESNT_EXIST, DB_FIELD_NAME_WRONG_FIELD),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void deleteFromOneRowTableButWhereHasWrongValueTypeIntegerForRightString() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  DELETE
        //    FROM person
        //   WHERE name = 1
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbTabPerson.delete(
                        dbRec -> dbRec.getValue(DB_FIELD_NAME_NAME).equals(1)
                )
        );

        Assertions.assertEquals(
                String.format(ERROR_VALUE_IS_WRONG_TYPE, 1, String.class),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void deleteFromOneRowTableButWhereHasWrongValueTypeStringForRightInteger() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  DELETE
        //    FROM person
        //   WHERE id = 'Bob'
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbTabPerson.delete(
                        dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals("Bob")
                )
        );

        Assertions.assertEquals(
                String.format(ERROR_VALUE_IS_WRONG_TYPE, "Bob", Integer.class),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }
}
