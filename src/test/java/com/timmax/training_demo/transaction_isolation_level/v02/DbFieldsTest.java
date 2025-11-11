package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbFields.ERROR_COLUMN_SPECIFIED_MORE_THAN_ONCE;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbFields.ERROR_TYPE_DOES_NOT_EXIST;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbFieldsTest {
    @Test
    void createDbFieldsButWithDuplicateNameColumns() {
        //  Prepare column list for
        //  CREATE TABLE person(
        //      id INT,
        //      id INT
        //  )
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> new DbFields(
                        DB_FIELD_ID,
                        DB_FIELD_ID
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
    public void createDbFieldsButWithWrongTypeInColumn() {
        //  Prepare column list for
        //  CREATE TABLE person(
        //      id INT,
        //      wrong_field null
        //  )
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> new DbFields(
                        DB_FIELD_ID,
                        DB_FIELD_WRONG_FIELD
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
    void createDbFieldsButWithDuplicateNameColumnsAndWrongTypeInColumn() {
        //  Prepare column list for
        //  CREATE TABLE person(
        //      id INT,
        //      id INT,
        //      wrong_field null
        //  )
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> new DbFields(
                        DB_FIELD_ID,
                        DB_FIELD_ID,
                        DB_FIELD_WRONG_FIELD
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
