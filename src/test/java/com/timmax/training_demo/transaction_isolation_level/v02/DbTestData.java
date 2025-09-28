package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.Map;

public class DbTestData {
    public static final DbTab dbTablePersonEmpty = new DbTab(
            "person",
            new DbFields(
                    new DbField("id", Integer.class),
                    new DbField("name", String.class)
            )
    );

    public static final DbTab dbTablePersonWithOneRow = new DbTab(
            "person",
            new DbFields(
                    new DbField("id", Integer.class),
                    new DbField("name", String.class)
            )
    );
    static {
        dbTablePersonWithOneRow.insert(
                new DbFieldNames("id", "name"),
                new DbRec(Map.of("id", 1, "name", "Bob"))
        );
    }
}
