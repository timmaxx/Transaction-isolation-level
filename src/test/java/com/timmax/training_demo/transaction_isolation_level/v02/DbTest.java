package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbTest.class);

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
        Assertions.assertThrows(DbDataAccessException.class, () -> dbTabPersonEmpty.insert(null));
    }

    @Test
    public void updateReadOnlyTable() {
        Assertions.assertThrows(DbDataAccessException.class, () -> dbTabPersonEmpty.update(null));
    }

    @Test
    public void deleteFromReadOnlyTable() {
        Assertions.assertThrows(DbDataAccessException.class, () -> dbTabPersonEmpty.delete(null));
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
    public void insertOneRowIntoEmptyTableWithWrongFieldName() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person (
        //      wrong_name, name
        //      ) values (
        //      1, "Bob"
        //  )
        Assertions.assertThrows(DbSQLException.class, () ->
                dbTabPerson.insert(
                        new DbRec(Map.of(DB_FIELD_NAME_WRONG_FIELD, 1, DB_FIELD_NAME_NAME, "Bob"))
                )
        );
    }

    @Test
    public void insertOneRowIntoEmptyTableWithWrongTypeValue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  INSERT
        //    INTO person (
        //      id, name
        //      ) values (
        //      "B", 999
        //  )
        Assertions.assertThrows(DbSQLException.class, () ->
                dbTabPerson.insert(
                        new DbRec(Map.of(DB_FIELD_NAME_ID, "B", DB_FIELD_NAME_NAME, 999))
                )
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
    public void updateTwoRowsTableWhereWrongField() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  UPDATE person
        //     SET wrong_field = name || ' ' || name
        //   WHERE id = 2
        Assertions.assertThrows(DbSQLException.class, () ->
                dbTabPerson.update(
                        dbRec -> Map.of(
                                DB_FIELD_NAME_WRONG_FIELD, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        ),
                        dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(2)
                )
        );
    }
}
