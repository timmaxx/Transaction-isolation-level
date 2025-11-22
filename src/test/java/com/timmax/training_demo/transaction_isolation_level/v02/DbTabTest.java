package com.timmax.training_demo.transaction_isolation_level.v02;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbTabTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbTabTest.class);

    @Test
    public void dbTabCopyMustHaveOwnDbRecCopy() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithOneRow, false);

        Assertions.assertNotSame(
                dbTabPersonWithOneRow.rowId_DbRec_Map.values().stream().findAny().get(),
                dbTabPerson.rowId_DbRec_Map.values().stream().findAny().get()
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
                DB_FIELDS,
                true
        );

        Assertions.assertEquals(dbTabPersonEmpty, dbTabPerson);
    }
}
