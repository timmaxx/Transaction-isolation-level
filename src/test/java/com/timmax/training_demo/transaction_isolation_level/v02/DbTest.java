package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbRec.COLUMN_DOESNT_EXIST;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbRec.INVALID_INPUT_SYNTAX_FOR_COLUMN;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTab.*;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbTest.class);

    private final static String EXCEPTION_MESSAGE_DOESNT_MATCH = "The exception message does not match the expected one.";

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

        //  DbSelect dbSelect = dbTabPerson.select();
        //  Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void insertIntoReadOnlyTable() {
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                () -> dbTabPersonEmpty.insert(null)
        );
        Assertions.assertEquals(
                String.format(TABLE_IS_RO + " " + YOU_CANNOT_INSERT, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void updateReadOnlyTable() {
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                () -> dbTabPersonEmpty.update(null)
        );
        Assertions.assertEquals(
                String.format(TABLE_IS_RO + " " + YOU_CANNOT_UPDATE, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void deleteFromReadOnlyTable() {
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                () -> dbTabPersonEmpty.delete(null)
        );
        Assertions.assertEquals(
                String.format(TABLE_IS_RO + " " + YOU_CANNOT_DELETE, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void selectFromEmptyTable() {
        //  SELECT *
        //    FROM person
        DbSelect dbSelect = dbTabPersonEmpty.select();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void insertOneRowIntoEmptyTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person (
        //      id, name
        //      ) values (
        //      1, "Bob"
        //  )
        dbTabPerson.insert(dbRec1_Bob);

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void insertOneRowIntoEmptyTableButFieldsHasWrongField() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person (
        //      wrong_name, name
        //      ) values (
        //      1, "Bob"
        //  )
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbTabPerson.insert(
                        new DbRec(Map.of(DB_FIELD_NAME_WRONG_FIELD, 1, DB_FIELD_NAME_NAME, "Bob"))
                )
        );
        Assertions.assertEquals(
                String.format("\n" + COLUMN_DOESNT_EXIST + "\n", DB_FIELD_NAME_WRONG_FIELD),
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
        //      ) values (
        //      "B", 999
        //  )
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbTabPerson.insert(
                        new DbRec(Map.of(DB_FIELD_NAME_ID, "B", DB_FIELD_NAME_NAME, 999))
                )
        );
        Assertions.assertEquals(
                String.format("\n" +
                                INVALID_INPUT_SYNTAX_FOR_COLUMN + "\n" +
                                INVALID_INPUT_SYNTAX_FOR_COLUMN + "\n",
                        String.class, DB_FIELD_NAME_NAME, 999,
                        Integer.class, DB_FIELD_NAME_ID, "B"
                ),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
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
        dbTabPerson.delete(dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(2));

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void updateTwoRowsTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  UPDATE person
        //     SET name = name || ' ' || name
        dbTabPerson.update(
                dbRec -> Map.of(
                        DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                )
        );

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonWithTwoRowsAllUpdated, dbSelect);
    }

    @Test
    public void updateTwoRowsTableWhereIdEq2() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  UPDATE person
        //     SET name = name || ' ' || name
        //   WHERE id = 2
        dbTabPerson.update(
                dbRec -> Map.of(
                        DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                ),
                dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(2)
        );

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonWithTwoRowsIdEq2Updated, dbSelect);
    }

    @Test
    public void updateTwoRowsTableButSetHasWrongField() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  UPDATE person
        //     SET wrong_field = name || ' ' || name
        //   WHERE id = 2
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbTabPerson.update(
                        dbRec -> Map.of(
                                DB_FIELD_NAME_WRONG_FIELD, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        ),
                        dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(2)
                )
        );
        Assertions.assertEquals(
                String.format("\n" + COLUMN_DOESNT_EXIST + "\n", DB_FIELD_NAME_WRONG_FIELD),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void updateTwoRowsTableButWhereHasWrongField() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  UPDATE person
        //     SET name = name || ' ' || name
        //   WHERE wrong_field = 2
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                () -> dbTabPerson.update(
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        ),
                        dbRec -> dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD).equals(2)
                )
        );
        Assertions.assertEquals(
                String.format(COLUMN_DOESNT_EXIST, DB_FIELD_NAME_WRONG_FIELD),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void updateTwoRowsTableButSetHasWrongFieldsAndWhereHasWrongFields() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  UPDATE person
        //     SET name = name || ' ' || name
        //   WHERE wrong_field = 2 or wrong_field_2 = 'Bob'
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                //  Не получится собрать текст для исключения, в котором можно было-бы описать все ошибки и в where и в set.
                //  В этом месте сработает одно исключение на первой части where:
                //      dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD).equals(2)
                //  Если его закомментировать, возникнет исключение на второй части where:
                //      dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD_2).equals("Bob")
                //  Если-же вообще без where,
                //      то исключение возникнет при попытке взять значение несуществующего поля перед тем, как вызовется Map.of
                //          dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD_2)
                //      если-же значения для мапы будут рассчитаны, мапа создастся, но ошибка в имени будет найдена в ключах мапы,
                //      тогда исключение возникнет в setAll.
                //          DB_FIELD_NAME_WRONG_FIELD.
                () -> dbTabPerson.update(
                        dbRec -> Map.of(
                                DB_FIELD_NAME_WRONG_FIELD, dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD_2) + " " + dbRec.getValue(DB_FIELD_NAME_NAME),
                                DB_FIELD_NAME_WRONG_FIELD_2, "   "
                        ),
                        dbRec -> (dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD).equals(2) || dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD_2).equals("Bob"))
                )
        );
        Assertions.assertEquals(
                String.format(COLUMN_DOESNT_EXIST, DB_FIELD_NAME_WRONG_FIELD),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }
}
