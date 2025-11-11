package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbRec.*;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbRecTest {
    //  1.  Wrong column name:
    //  1.1.    SELECT, UPDATE, DELETE
    //           WHERE wrong_field = 1
    //  1.2.    UPDATE
    //             SET wrong_field = 1
    //  1.3.    INSERT
    //            INTO person (
    //                   wrong_name, name
    //                 )
    //          VALUES (
    //                   1, "Bob"
    //                 )

    @Test
    void createDbRecButFieldsHasWrongColumn() {
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> new DbRec(
                        DB_FIELDS,
                        Map.of(DB_FIELD_NAME_WRONG_FIELD, 1)
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
    void dBRecDoesntHaveWrongNameField() {
        //   WHERE wrong_field = 1
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbRec1_Bob_email.getValue(DB_FIELD_NAME_WRONG_FIELD).equals(1)
        );

        Assertions.assertEquals(
                String.format(ERROR_COLUMN_DOESNT_EXIST, DB_FIELD_NAME_WRONG_FIELD),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void createDbRecButItHasWrongField() {
        //     SET wrong_field = name || " " || name
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> new DbRec(DB_FIELDS,
                        Map.of(DB_FIELD_NAME_WRONG_FIELD,
                                dbRec1_Bob_email.getValue(DB_FIELD_NAME_NAME) + " " + dbRec1_Bob_email.getValue(DB_FIELD_NAME_NAME)
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

    //  2.  Wrong type errors:
    //  2.1.     WHERE
    //           WHERE id = "Bob"
    //           WHERE name = 1
    //  2.2.       SET
    //             SET id = "Bob"
    //             SET name = 1
    //  2.3.    INSERT INTO t (id, name) values ("Bob", 1)

    @Test
    void createDbRecButWithValueForIdIsWrongType() {
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_ID, "Bob"))
        );

        Assertions.assertEquals(
                String.format("\n" +
                                ERROR_INVALID_INPUT_SYNTAX_FOR_COLUMN + "\n",
                        Integer.class, DB_FIELD_NAME_ID, "Bob"),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    void createDbRecButValueForNameIsWrongType() {
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_NAME, 1))
        );

        Assertions.assertEquals(
                String.format("\n" +
                                ERROR_INVALID_INPUT_SYNTAX_FOR_COLUMN + "\n",
                        String.class, DB_FIELD_NAME_NAME, 1),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    void createDbRecButValuesForIdAndNameAreWrongType() {
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_ID, "Bob", DB_FIELD_NAME_NAME, 1))
        );

        Assertions.assertEquals(
                String.format("\n" +
                                ERROR_INVALID_INPUT_SYNTAX_FOR_COLUMN + "\n" +
                                ERROR_INVALID_INPUT_SYNTAX_FOR_COLUMN + "\n",
                        Integer.class, DB_FIELD_NAME_ID, "Bob",
                        String.class, DB_FIELD_NAME_NAME, 1),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    //  3.  Nullable errors:
    @Test
    void createDbRecButIdIsNullAndNameIsNull() {
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> new DbRec(DB_FIELDS, Map.of())
        );

        Assertions.assertEquals(
                String.format("\n" +
                                ERROR_NULL_VALUE_IN_COLUMN_VIOLATES_NOT_NULL_CONSTRAINT + "\n" +
                                ERROR_NULL_VALUE_IN_COLUMN_VIOLATES_NOT_NULL_CONSTRAINT + "\n",
                        DB_FIELD_NAME_ID,
                        DB_FIELD_NAME_NAME),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    void createDbRecButNameIsNull() {
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_ID, 4))
        );

        Assertions.assertEquals(
                String.format("\n" +
                        ERROR_NULL_VALUE_IN_COLUMN_VIOLATES_NOT_NULL_CONSTRAINT + "\n",
                        DB_FIELD_NAME_NAME),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    void createDbRecButIdIsNull() {
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_NAME, "Tom"))
        );

        Assertions.assertEquals(
                String.format("\n" +
                        ERROR_NULL_VALUE_IN_COLUMN_VIOLATES_NOT_NULL_CONSTRAINT + "\n",
                        DB_FIELD_NAME_ID),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }
}
