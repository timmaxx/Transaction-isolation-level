package com.timmax.training_demo.transaction_isolation_level.v02;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.dbTablePersonEmpty;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.dbTablePersonWithOneRow;

public class DbTest {
    @Test
    public void createDbTab() {
        //  CREATE TABLE person(
        //      id INT,
        //      name VARCHAR(50)
        //  )

        DbTab dbTablePerson = new DbTab(
                "person",
                new DbFields(
                        new DbField("id", Integer.class),
                        new DbField("name", String.class)
                )
        );

        Assertions.assertEquals(dbTablePersonEmpty, dbTablePerson);
    }

    @Test
    public void insertOneRowIntoEmptyTable() {
        DbTab dbTablePerson = new DbTab(
                "person",
                new DbFields(
                        new DbField("id", Integer.class),
                        new DbField("name", String.class)
                )
        );

        //  INSERT INTO person (
        //      id, name
        //      ) values (
        //      1, "Bob"
        //  )
        dbTablePerson.insert(
                new DbFieldNames("id", "name"),
                new DbRec(Map.of("id", 1, "name", "Bob"))
        );

        Assertions.assertEquals(dbTablePersonWithOneRow, dbTablePerson);
    }
}
