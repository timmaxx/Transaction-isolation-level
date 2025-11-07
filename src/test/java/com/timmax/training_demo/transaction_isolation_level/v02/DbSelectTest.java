package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbRec.ERROR_COLUMN_DOESNT_EXIST;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbSelectTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbSelectTest.class);

    @Test
    public void selectFromEmptyTable() {
        //  SELECT *
        //    FROM person
        DbSelect dbSelect = dbTabPersonEmpty.select();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void selectFromOneRowTable() {
        //  SELECT *
        //    FROM person
        DbSelect dbSelect = dbTabPersonWithOneRow.select();

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void selectFromEmptyTableWhereIdEq1() {
        //  SELECT *
        //    FROM person
        //   WHERE id = 1
        DbSelect dbSelect = dbTabPersonEmpty.select(
                dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(1)
        );

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void selectFromOneRowTableWhereIdEq1() {
        //  SELECT *
        //    FROM person
        //   WHERE id = 1
        DbSelect dbSelect = dbTabPersonWithOneRow.select(
                dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(1)
        );

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void selectFromOneRowTableButWhereHasWrongNameField() {
        //  SELECT *
        //    FROM person
        //   WHERE wrong_field = 2
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbTabPersonWithOneRow.select(
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
    public void selectFromOneRowTableButWhereHasWrongNameFields() {
        //  SELECT *
        //    FROM person
        //   WHERE wrong_field = 2
        //      or wrong_field_2 = 'Bob'
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                //  См. комментарии к DbUpdateTest :: updateTwoRowsTableButSetHasWrongFieldsAndWhereHasWrongNameFields
                () -> dbTabPersonWithOneRow.select(
                        dbRec -> (
                                dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD).equals(2) ||
                                        dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD_2).equals("Bob")
                        )

                )
        );

        Assertions.assertEquals(
                String.format(ERROR_COLUMN_DOESNT_EXIST, DB_FIELD_NAME_WRONG_FIELD),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    //  ToDo:   make tests with wrong where (invalid data type).
}
