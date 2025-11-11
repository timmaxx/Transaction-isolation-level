package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbFieldValue.ERROR_VALUE_IS_WRONG_TYPE;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbFieldValueTest {
    @Test
    void columnIsStringButItComparedWithInteger() {
        //   WHERE name = 1
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbRec1_Bob_email.getValue(DB_FIELD_NAME_NAME).equals(1)
        );

        Assertions.assertEquals(
                String.format(ERROR_VALUE_IS_WRONG_TYPE, 1, String.class),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    void columnIsIntegerButItComparedWithString() {
        //   WHERE id = "Bob"
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbRec1_Bob_email.getValue(DB_FIELD_NAME_ID).equals("Bob")
        );

        Assertions.assertEquals(
                String.format(ERROR_VALUE_IS_WRONG_TYPE, "Bob", Integer.class),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }
}
