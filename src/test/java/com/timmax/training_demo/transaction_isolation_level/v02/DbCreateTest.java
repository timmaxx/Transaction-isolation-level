package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbFields.ERROR_COLUMN_SPECIFIED_MORE_THAN_ONCE;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbFields.ERROR_TYPE_DOES_NOT_EXIST;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbCreateTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbCreateTest.class);

    @Test
    public void dbTabCopyMustHaveOwnDbRec0Copy() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithOneRow, false);

        Assertions.assertNotSame(
                dbTabPersonWithOneRow.dbRecs.stream().findAny().get(),
                dbTabPerson.dbRecs.stream().findAny().get()
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
                new DbFields(
                        DB_FIELD_ID,
                        DB_FIELD_NAME
                ), true
        );

        Assertions.assertEquals(dbTabPersonEmpty, dbTabPerson);
    }

    @Test
    public void createTableWithDuplicateNameColumns() {
        //  CREATE TABLE person(
        //      id INT,
        //      id INT
        //  )
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> new DbTab(
                        DB_TAB_NAME_PERSON,
                        new DbFields(
                                DB_FIELD_ID,
                                DB_FIELD_ID
                        ), true
                )
        );

        Assertions.assertEquals(
                String.format("\n" +
                        ERROR_COLUMN_SPECIFIED_MORE_THAN_ONCE + "\n",
                        DB_FIELD_ID.getDbFieldName()
                ),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void createTableWithWrongTypeInColumn() {
        //  CREATE TABLE person(
        //      id INT,
        //      wrong_field null
        //  )
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> new DbTab(
                        DB_TAB_NAME_PERSON,
                        new DbFields(
                                DB_FIELD_ID,
                                DB_FIELD_WRONG_FIELD
                        ), true
                )
        );

        Assertions.assertEquals(
                String.format("\n" +
                        ERROR_TYPE_DOES_NOT_EXIST + "\n",
                        "null", DB_FIELD_NAME_WRONG_FIELD
                ),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void createTableWithDuplicateNameColumnsAndWrongTypeInColumn() {
        //  CREATE TABLE person(
        //      id INT,
        //      id INT,
        //      wrong_field null
        //  )
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> new DbTab(
                        DB_TAB_NAME_PERSON,
                        new DbFields(
                                DB_FIELD_ID,
                                DB_FIELD_ID,
                                DB_FIELD_WRONG_FIELD
                        ), true
                )
        );

        Assertions.assertEquals(
                String.format("\n" +
                                ERROR_COLUMN_SPECIFIED_MORE_THAN_ONCE + "\n" +
                                ERROR_TYPE_DOES_NOT_EXIST + "\n",
                        DB_FIELD_NAME_ID,
                        "null", DB_FIELD_NAME_WRONG_FIELD
                ),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }
}
