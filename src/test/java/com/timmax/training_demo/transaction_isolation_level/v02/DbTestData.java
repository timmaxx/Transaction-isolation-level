package com.timmax.training_demo.transaction_isolation_level.v02;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DbTestData {
    protected static final Logger logger = LoggerFactory.getLogger(DbTestData.class);

    final static String EXCEPTION_MESSAGE_DOESNT_MATCH = "The exception message does not match the expected one.";

    public static final DbFieldName DB_FIELD_NAME_WRONG_FIELD = new DbFieldName("wrong_field");
    public static final DbFieldName DB_FIELD_NAME_WRONG_FIELD_2 = new DbFieldName("wrong_field_2");
    public static final DbField<Object> DB_FIELD_WRONG_FIELD = new DbField<>(DB_FIELD_NAME_WRONG_FIELD, new DbFieldDefinition<>(null));

    public static final DbFieldName DB_FIELD_NAME_ID = new DbFieldName("id");
    public static final DbField<Integer> DB_FIELD_ID = new DbField<>(DB_FIELD_NAME_ID, new DbFieldDefinition<>(Integer.class));

    public static final DbFieldName DB_FIELD_NAME_NAME = new DbFieldName("name");
    public static final DbField<String> DB_FIELD_NAME = new DbField<>(DB_FIELD_NAME_NAME, new DbFieldDefinition<>(String.class));

    public static final DbTabName DB_TAB_NAME_PERSON = new DbTabName("person");

    public static final DbFields DB_FIELDS = new DbFields(DB_FIELD_ID, DB_FIELD_NAME);

    public static final DbRec dbRec1_Bob = new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_ID, 1, DB_FIELD_NAME_NAME, "Bob"));
    public static final DbRec dbRec2_Alice = new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_ID, 2, DB_FIELD_NAME_NAME, "Alice"));
    public static final DbRec dbRec1_BobBob = new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_ID, 1, DB_FIELD_NAME_NAME, "Bob Bob"));
    public static final DbRec dbRec2_AliceAlice = new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_ID, 2, DB_FIELD_NAME_NAME, "Alice Alice"));


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

    public static final DbRec dbRecNull_Null = new DbRec(DB_FIELDS, Map.of());

    public static final DbRec dbRec3_Null = new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_ID, 3));
    //  Использовать Map.Of() со значениями null нельзя, поэтому применил HashMap.
    // public static final DbRec dbRec3_Null2 = new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_ID, 3, DB_FIELD_NAME_NAME, null));
    public static final DbRec dbRec3_Null2;
    static {
        Map<DbFieldName, Object> m = new HashMap<>();
        m.put(DB_FIELD_NAME_ID, 3);
        m.put(DB_FIELD_NAME_NAME, null);
        dbRec3_Null2 = new DbRec(DB_FIELDS, m);
    }

    public static final DbTab dbTabPersonWithOneRowNameIsNull = new DbTab(dbTabPersonEmpty, true, Set.of(dbRec3_Null));
    public static final DbSelect dbSelectPersonWithOneRowNameIsNull = dbTabPersonWithOneRowNameIsNull.select();

    public static final DbTab dbTabPersonWithOneRowIdAndNameAreNull = new DbTab(dbTabPersonEmpty, true, Set.of(dbRecNull_Null));
    public static final DbSelect dbSelectPersonWithOneRowIdAndNameAreNull = dbTabPersonWithOneRowIdAndNameAreNull.select();
}
