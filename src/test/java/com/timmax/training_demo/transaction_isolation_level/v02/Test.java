package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.Map;

public class Test {
    public static void main(String[] args) {
        DbField dbFieldId = new DbField("id", Integer.class);
        DbField dbFieldName = new DbField("name", String.class);

        DbFields dbFields = new DbFields();
        dbFields.putField(dbFieldId);
        dbFields.putField(dbFieldName);

        DbTab dbTablePerson = new DbTab("person", dbFields);

        System.out.println(dbTablePerson);

        dbTablePerson.insert(
                new DbFieldNames("id", "name"),
                new DbRec(Map.of("id", 1, "name", "Bob"))
        );

        System.out.println(dbTablePerson);

        dbTablePerson.insert(
                new DbFieldNames("id", "name"),
                new DbRec(Map.of("id", 2, "name", "Alice"))
        );

        System.out.println(dbTablePerson);
    }
}
