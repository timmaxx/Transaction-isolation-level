package com.timmax.training_demo.transaction_isolation_level.v02;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class DbTestData {
    protected static final Logger logger = LoggerFactory.getLogger(DbTestData.class);

    public static final DbFieldName DB_FIELD_NAME_WRONG_FIELD = new DbFieldName("wrong_field");

    public static final DbFieldName DB_FIELD_NAME_ID = new DbFieldName("id");
    public static final DbField<Integer> DB_FIELD_ID = new DbField<>(DB_FIELD_NAME_ID, Integer.class);

    public static final DbFieldName DB_FIELD_NAME_NAME = new DbFieldName("name");
    public static final DbField<String> DB_FIELD_NAME = new DbField<>(DB_FIELD_NAME_NAME, String.class);

    public static final DbTabName DB_TAB_NAME_PERSON = new DbTabName("person");

    public static final DbRec dbRec1_Bob = new DbRec(Map.of(DB_FIELD_NAME_ID, 1, DB_FIELD_NAME_NAME, "Bob"));
    public static final DbRec dbRec2_Alice = new DbRec(Map.of(DB_FIELD_NAME_ID, 2, DB_FIELD_NAME_NAME, "Alice"));
    public static final DbRec dbRec1_BobBob = new DbRec(Map.of(DB_FIELD_NAME_ID, 1, DB_FIELD_NAME_NAME, "Bob Bob"));
    public static final DbRec dbRec2_AliceAlice = new DbRec(Map.of(DB_FIELD_NAME_ID, 2, DB_FIELD_NAME_NAME, "Alice Alice"));


    public static final DbTab dbTabPersonEmpty = new DbTab(
            DB_TAB_NAME_PERSON,
            new DbFields(
                    DB_FIELD_ID,
                    DB_FIELD_NAME
            ),
            true
    );
    public static final DbSelect dbSelectPersonEmpty = dbTabPersonEmpty.select();

    public static final DbTab dbTabPersonWithOneRow = new DbTab(dbTabPersonEmpty, true, Set.of(dbRec1_Bob));
    public static final DbSelect dbSelectPersonWithOneRow = dbTabPersonWithOneRow.select();

    public static final DbTab dbTabPersonWithTwoRows = new DbTab(dbTabPersonWithOneRow, true, Set.of(dbRec2_Alice));

    public static final DbTab dbTabPersonWithTwoRowsAllUpdated = new DbTab(dbTabPersonEmpty, true, Set.of(dbRec1_BobBob, dbRec2_AliceAlice));
    public static final DbSelect dbSelectPersonWithTwoRowsAllUpdated = dbTabPersonWithTwoRowsAllUpdated.select();

    public static final DbTab dbTabPersonWithTwoRowsIdEq2Updated = new DbTab(dbTabPersonWithOneRow, true, Set.of(dbRec2_AliceAlice));
    public static final DbSelect dbSelectPersonWithTwoRowsIdEq2Updated = dbTabPersonWithTwoRowsIdEq2Updated.select();
}
