package com.timmax.training_demo.transaction_isolation_level.v02;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbTest.class);

/*
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
        Assertions.assertThrows(DataAccessException.class, () -> dbTabPersonEmpty.insert(null));
    }

    @Test
    public void updateReadOnlyTable() {
        Assertions.assertThrows(DataAccessException.class, () -> dbTabPersonEmpty.update(null));
    }

    @Test
    public void deleteFromReadOnlyTable() {
        Assertions.assertThrows(DataAccessException.class, () -> dbTabPersonEmpty.delete(null));
    }

    @Test
    public void selectFromEmptyTable() {
        //  SELECT *
        //    FROM person
        DbSelect dbSelect = dbTabPersonEmpty.select();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void insertOneRowIntoEmptyTable() throws DataAccessException {
        DbTab dbTabPerson = new DbTab(
                DB_TAB_NAME_PERSON,
                new DbFields(
                        DB_FIELD_ID,
                        DB_FIELD_NAME
                ), false
        );

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
        DbSelect dbSelect = dbTabPersonEmpty.select(dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(1));

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void selectFromOneRowTableWhereIdEq1() {
        //  SELECT *
        //    FROM person
        //   WHERE id = 1
        DbSelect dbSelect = dbTabPersonWithOneRow.select(dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(1));

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void deleteFromEmptyTable() throws DataAccessException {
        DbTab dbTabPerson = new DbTab(
                DB_TAB_NAME_PERSON,
                new DbFields(
                        DB_FIELD_ID,
                        DB_FIELD_NAME
                ), false
        );

        //  DELETE
        //    FROM person
        dbTabPerson.delete();

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromOneRowTable() throws DataAccessException {
        DbTab dbTabPerson = new DbTab(
                DB_TAB_NAME_PERSON,
                new DbFields(
                        DB_FIELD_ID,
                        DB_FIELD_NAME
                ), false
        );
        dbTabPerson.insert(dbRec1_Bob);

        //  DELETE
        //    FROM person
        dbTabPerson.delete();

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2() throws DataAccessException {
        DbTab dbTabPerson = new DbTab(
                DB_TAB_NAME_PERSON,
                new DbFields(
                        DB_FIELD_ID,
                        DB_FIELD_NAME
                ), false
        );
        dbTabPerson.insert(dbRec1_Bob);
        dbTabPerson.insert(dbRec2_Alice);

        //  DELETE
        //    FROM person
        //   WHERE id = 1
        dbTabPerson.delete(dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(2));

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }
*/

    @Test
    public void updateTwoRowsTable() throws DataAccessException {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        logger.info("dbTabPerson = {}", dbTabPerson);

        //  UPDATE person
        //     SET name = name || ' ' || name
        dbTabPerson.update(
                dbRec -> new DbRec(
                        Map.of(
                                DB_FIELD_NAME_ID, dbRec.getValue(DB_FIELD_NAME_ID),
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        )
                )
        );

        DbSelect dbSelect = dbTabPerson.select();

        logger.info("dbSelectPersonWithTwoRowsAllUpdated = {}", dbSelectPersonWithTwoRowsAllUpdated);
        logger.info("                           dbSelect = {}", dbSelect);
        logger.info("dbTabPersonWithTwoRows = {}", dbTabPersonWithTwoRows);
        Assertions.assertEquals(dbSelectPersonWithTwoRowsAllUpdated, dbSelect);
    }

    @Test
    public void updateTwoRowsTableWhereIdEq2() throws DataAccessException {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        logger.info("dbTabPersonWithTwoRows = {}", dbTabPersonWithTwoRows);
        logger.info("dbTabPerson            = {}", dbTabPerson);
/*
        DbTab dbTabPerson = new DbTab(
                DB_TAB_NAME_PERSON,
                new DbFields(
                        DB_FIELD_ID,
                        DB_FIELD_NAME
                ), false
        );
        dbTabPerson.insert(dbRec1_Bob);
        dbTabPerson.insert(dbRec2_Alice);
*/

        //  UPDATE person
        //     SET name = name || ' ' || name
        //   WHERE id = 2
        dbTabPerson.update(
                dbRec -> new DbRec(
                        Map.of(
                                DB_FIELD_NAME_ID, dbRec.getValue(DB_FIELD_NAME_ID),
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        )
                ), dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(2)
        );

        DbSelect dbSelect = dbTabPerson.select();

        logger.info("dbTabPersonWithTwoRows = {}", dbTabPersonWithTwoRows);
        logger.info("dbTabPerson            = {}", dbTabPerson);
        Assertions.assertEquals(dbSelectPersonWithTwoRowsIdEq2Updated, dbSelect);
    }
}
