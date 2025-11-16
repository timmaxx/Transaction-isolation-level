package com.timmax.training_demo.transaction_isolation_level.v02;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DbTestData {
    protected static final Logger logger = LoggerFactory.getLogger(DbTestData.class);

    final static String EXCEPTION_MESSAGE_DOESNT_MATCH = "The exception message does not match the expected one.";

    public static final DbFieldName DB_FIELD_NAME_WRONG_FIELD = new DbFieldName("wrong_field");
    public static final DbField<Object> DB_FIELD_WRONG_FIELD = new DbField<>(DB_FIELD_NAME_WRONG_FIELD, new DbFieldDefinition<>(null));

    public static final DbFieldName DB_FIELD_NAME_ID = new DbFieldName("id");
    public static final DbField<Integer> DB_FIELD_ID = new DbField<>(DB_FIELD_NAME_ID, new DbFieldDefinition<>(Integer.class, false));

    public static final DbFieldName DB_FIELD_NAME_NAME = new DbFieldName("name");
    public static final DbField<String> DB_FIELD_NAME = new DbField<>(DB_FIELD_NAME_NAME, new DbFieldDefinition<>(String.class, false));

    public static final DbFieldName DB_FIELD_NAME_EMAIL = new DbFieldName("email");
    public static final DbField<String> DB_FIELD_EMAIL = new DbField<>(DB_FIELD_NAME_EMAIL, new DbFieldDefinition<>(String.class, true));

    public static final DbTabName DB_TAB_NAME_PERSON = new DbTabName("person");

    public static final DbFields DB_FIELDS = new DbFields(DB_FIELD_ID, DB_FIELD_NAME, DB_FIELD_EMAIL);

    public static final DbRec dbRec1_Bob_email = new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_ID, 1, DB_FIELD_NAME_NAME, "Bob", DB_FIELD_NAME_EMAIL, "@"));
    public static final DbRec dbRec2_Alice_email = new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_ID, 2, DB_FIELD_NAME_NAME, "Alice", DB_FIELD_NAME_EMAIL, "@"));
    public static final DbRec dbRec1_BobBob_email = new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_ID, 1, DB_FIELD_NAME_NAME, "Bob Bob", DB_FIELD_NAME_EMAIL, "@"));
    public static final DbRec dbRec2_AliceAlice_email = new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_ID, 2, DB_FIELD_NAME_NAME, "Alice Alice", DB_FIELD_NAME_EMAIL, "@"));

    public static final DbTab dbTabPersonEmpty = new DbTab(
            DB_TAB_NAME_PERSON,
            DB_FIELDS,
            true
    );

    public static final DbSelect dbSelectPersonEmpty = dbTabPersonEmpty.select();

    public static final DbTab dbTabPersonWithOneRow = new DbTab(dbTabPersonEmpty, true, List.of(dbRec1_Bob_email));
    public static final DbSelect dbSelectPersonWithOneRow = dbTabPersonWithOneRow.select();

    public static final DbTab dbTabPersonWithTwoRows = new DbTab(dbTabPersonWithOneRow, true, List.of(dbRec2_Alice_email));
    public static final DbSelect dbSelectPersonWithTwoRows = dbTabPersonWithTwoRows.select();

    public static final DbTab dbTabPersonWithTwoRowsAllUpdated = new DbTab(dbTabPersonEmpty, true, List.of(dbRec1_BobBob_email, dbRec2_AliceAlice_email));
    public static final DbSelect dbSelectPersonWithTwoRowsAllUpdated = dbTabPersonWithTwoRowsAllUpdated.select();

    public static final DbTab dbTabPersonWithTwoRowsIdEq2Updated = new DbTab(dbTabPersonWithOneRow, true, List.of(dbRec2_AliceAlice_email));
    public static final DbSelect dbSelectPersonWithTwoRowsIdEq2Updated = dbTabPersonWithTwoRowsIdEq2Updated.select();

    public static final DbRec dbRec3_Tom_Null = new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_ID, 3, DB_FIELD_NAME_NAME, "Tom"));

    //  Использовать Map.Of() со значениями null нельзя, поэтому применил HashMap.
    // public static final DbRec dbRec3_Tom_Null2 = new DbRec(DB_FIELDS, Map.of(DB_FIELD_NAME_ID, 3, DB_FIELD_NAME_NAME, "Tom", DB_FIELD_NAME_EMAIL, null));
    public static final DbRec dbRec3_Tom_Null2;
    static {
        Map<DbFieldName, Object> m = new HashMap<>();
        m.put(DB_FIELD_NAME_ID, 3);
        m.put(DB_FIELD_NAME_NAME, "Tom");
        m.put(DB_FIELD_NAME_EMAIL, null);
        dbRec3_Tom_Null2 = new DbRec(DB_FIELDS, m);
    }

    public static final DbTab dbTabPersonWithOneRowNameIsNull = new DbTab(dbTabPersonEmpty, true, List.of(dbRec3_Tom_Null));
    public static final DbSelect dbSelectPersonWithOneRowEmailIsNull = dbTabPersonWithOneRowNameIsNull.select();
}
