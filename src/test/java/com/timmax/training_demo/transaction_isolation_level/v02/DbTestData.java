package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.Map;

public class DbTestData {
    public static final DbFieldName DB_FIELD_NAME_ID = new DbFieldName("id");
    public static final DbField<Integer> DB_FIELD_ID = new DbField<>(DB_FIELD_NAME_ID, Integer.class);

    public static final DbFieldName DB_FIELD_NAME_NAME = new DbFieldName("name");
    public static final DbField<String> DB_FIELD_NAME = new DbField<>(DB_FIELD_NAME_NAME, String.class);

    public static final DbTabName DB_TAB_NAME_PERSON = new DbTabName("person");

    public static final DbTab dbTabPersonEmpty;
    public static final DbSelect dbSelectPersonEmpty;
    static {
        dbTabPersonEmpty = new DbTab(
                DB_TAB_NAME_PERSON,
                new DbFields(
                        DB_FIELD_ID,
                        DB_FIELD_NAME
                )
        );
        dbTabPersonEmpty.setReadOnly();
        dbSelectPersonEmpty = dbTabPersonEmpty.select();
    }

    public static final DbTab dbTabPersonWithOneRow;
    public static final DbSelect dbSelectPersonWithOneRow;
    static {
        dbTabPersonWithOneRow = new DbTab(
                DB_TAB_NAME_PERSON,
                new DbFields(
                        DB_FIELD_ID,
                        DB_FIELD_NAME
                )
        );
        dbTabPersonWithOneRow.insert(
                new DbRec(Map.of(DB_FIELD_NAME_ID, 1, DB_FIELD_NAME_NAME, "Bob"))
        );
        dbTabPersonWithOneRow.setReadOnly();
        dbSelectPersonWithOneRow = dbTabPersonWithOneRow.select();
    }
}
