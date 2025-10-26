package com.timmax.training_demo.transaction_isolation_level.v02;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    //  ToDo:   make tests with wrong where.
}
