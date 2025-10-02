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
    public static final DbRec dbRec1_Bob = new DbRec(Map.of(DB_FIELD_NAME_ID, 1, DB_FIELD_NAME_NAME, "Bob"));
    static {
        dbTabPersonWithOneRow = new DbTab(
                DB_TAB_NAME_PERSON,
                new DbFields(
                        DB_FIELD_ID,
                        DB_FIELD_NAME
                )
        );
        dbTabPersonWithOneRow.insert(dbRec1_Bob);
        dbTabPersonWithOneRow.setReadOnly();
        dbSelectPersonWithOneRow = dbTabPersonWithOneRow.select();
    }


    public static final DbTab dbTabPersonWithTwoRows;
    public static final DbSelect dbSelectPersonWithTwoRows;
    public static final DbRec dbRec2_Alice = new DbRec(Map.of(DB_FIELD_NAME_ID, 2, DB_FIELD_NAME_NAME, "Alice"));
    static {
        dbTabPersonWithTwoRows = new DbTab(
                DB_TAB_NAME_PERSON,
                new DbFields(
                        DB_FIELD_ID,
                        DB_FIELD_NAME
                )
        );
        dbTabPersonWithTwoRows.insert(dbRec1_Bob);
        dbTabPersonWithTwoRows.insert(dbRec2_Alice);
        dbTabPersonWithTwoRows.setReadOnly();
        dbSelectPersonWithTwoRows = dbTabPersonWithTwoRows.select();
    }


    public static final DbTab dbTabPersonWithTwoRowsAllUpdated;
    public static final DbSelect dbSelectPersonWithTwoRowsAllUpdated;
    public static final DbRec dbRec1_BobBob = new DbRec(Map.of(DB_FIELD_NAME_ID, 1, DB_FIELD_NAME_NAME, "Bob Bob"));
    public static final DbRec dbRec2_AliceAlice = new DbRec(Map.of(DB_FIELD_NAME_ID, 2, DB_FIELD_NAME_NAME, "Alice Alice"));
    static {
        dbTabPersonWithTwoRowsAllUpdated = new DbTab(
                DB_TAB_NAME_PERSON,
                new DbFields(
                        DB_FIELD_ID,
                        DB_FIELD_NAME
                )
        );
        dbTabPersonWithTwoRowsAllUpdated.insert(dbRec1_BobBob);
        dbTabPersonWithTwoRowsAllUpdated.insert(dbRec2_AliceAlice);
        dbTabPersonWithTwoRowsAllUpdated.setReadOnly();
        dbSelectPersonWithTwoRowsAllUpdated = dbTabPersonWithTwoRowsAllUpdated.select();
    }


    public static final DbTab dbTabPersonWithTwoRowsIdEq2Updated;
    public static final DbSelect dbSelectPersonWithTwoRowsIdEq2Updated;
    static {
        dbTabPersonWithTwoRowsIdEq2Updated = new DbTab(
                DB_TAB_NAME_PERSON,
                new DbFields(
                        DB_FIELD_ID,
                        DB_FIELD_NAME
                )
        );
        dbTabPersonWithTwoRowsIdEq2Updated.insert(dbRec1_Bob);
        dbTabPersonWithTwoRowsIdEq2Updated.insert(dbRec2_AliceAlice);
        dbTabPersonWithTwoRowsIdEq2Updated.setReadOnly();
        dbSelectPersonWithTwoRowsIdEq2Updated = dbTabPersonWithTwoRowsIdEq2Updated.select();
    }
}
