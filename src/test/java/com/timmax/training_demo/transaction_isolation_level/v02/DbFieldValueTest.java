package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbFieldValue.ERROR_THIS_METHOD_EQUALS_IS_NOT_ALLOWED_FOR_THIS_CLASS;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbFieldValue.ERROR_VALUE_IS_WRONG_TYPE;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbFieldValueTest {
    @Test
    void columnIsStringAndItComparedWithEqualString() {
        //   WHERE name = "Bob" --  name is "Bob"
        boolean result = dbRec1_Bob_email.getValue(DB_FIELD_NAME_NAME).eq("Bob");

        Assertions.assertTrue(result);
    }

    @Test
    void columnIsStringAndItComparedWithNotEqualString() {
        //   WHERE name = "Alice"   --  name is "Bob"
        boolean result = dbRec1_Bob_email.getValue(DB_FIELD_NAME_NAME).eq("Alice");

        Assertions.assertFalse(result);
    }

    @Test
    void columnIsStringAndItComparedWithNull() {
        //   WHERE name = null --  name is "Bob"
        boolean result = dbRec1_Bob_email.getValue(DB_FIELD_NAME_NAME).eq(null);

        Assertions.assertFalse(result);
    }

    @Test
    void columnIsIntegerAndItComparedWithEqualInteger() {
        //   WHERE id = 1 --  id is 1
        boolean result = dbRec1_Bob_email.getValue(DB_FIELD_NAME_ID).eq(1);

        Assertions.assertTrue(result);
    }

    @Test
    void columnIsIntegerAndItComparedWithNotEqualInteger() {
        //   WHERE id = 2 --  id is 1
        boolean result = dbRec1_Bob_email.getValue(DB_FIELD_NAME_ID).eq(2);

        Assertions.assertFalse(result);
    }

    @Test
    void columnIsIntegerAndItComparedWithNull() {
        //   WHERE id = null --  id is 1
        boolean result = dbRec1_Bob_email.getValue(DB_FIELD_NAME_ID).eq(null);

        Assertions.assertFalse(result);
    }

    @Test
    void columnIsStringButItInvokeWrongMethod() {
        //   WHERE name ? "Bob" --  wrong method
        UnsupportedOperationException exception = Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> dbRec1_Bob_email.getValue(DB_FIELD_NAME_NAME).equals("Bob")
        );

        Assertions.assertEquals(
                String.format(ERROR_THIS_METHOD_EQUALS_IS_NOT_ALLOWED_FOR_THIS_CLASS),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    void columnIsStringButItComparedWithInteger() {
        //   WHERE name = 1 --  name is "Bob" and so types are different
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbRec1_Bob_email.getValue(DB_FIELD_NAME_NAME).eq(1)
        );

        Assertions.assertEquals(
                String.format(ERROR_VALUE_IS_WRONG_TYPE, 1, String.class),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    void columnIsIntegerButItComparedWithString() {
        //   WHERE id = "Bob"   --  id is 1  and so types are different
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbRec1_Bob_email.getValue(DB_FIELD_NAME_ID).eq("Bob")
        );

        Assertions.assertEquals(
                String.format(ERROR_VALUE_IS_WRONG_TYPE, "Bob", Integer.class),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }
}
