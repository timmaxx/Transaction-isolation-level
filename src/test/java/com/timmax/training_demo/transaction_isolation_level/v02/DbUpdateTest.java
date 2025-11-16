package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTab.*;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbUpdateTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbUpdateTest.class);

    @Test
    public void updateReadOnlyTable() {
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                () -> dbTabPersonEmpty.update(null)
        );

        Assertions.assertEquals(
                String.format(ERROR_TABLE_IS_RO_YOU_CANNOT_UPDATE, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void updateTwoRowsTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  UPDATE person
        //     SET name = name || " " || name
        dbTabPerson.update(
                dbRec -> Map.of(
                        DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                )
        );

        DbSelect dbSelect = dbTabPerson.select();

        // Assertions.assertEquals(dbSelectPersonWithTwoRowsAllUpdated, dbSelect);

        //  ToDo:   Нужно переделать. Т.к. сейчас в тесте вручную сортировать приходится.
        List<DbRec> values1 = new ArrayList<>(dbSelectPersonWithTwoRowsAllUpdated.dbRecs.values());
        List<DbRec> values2 = new ArrayList<>(dbSelect.dbRecs.values());
        values1.sort(Comparator.naturalOrder());
        values2.sort(Comparator.naturalOrder());

        Assertions.assertEquals(values1, values2);
    }

    @Test
    public void updateTwoRowsTableWhereIdEq2() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  UPDATE person
        //     SET name = name || " " || name
        //   WHERE id = 2
        dbTabPerson.update(
                dbRec -> Map.of(
                        DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                ),
                dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(2)
        );

        DbSelect dbSelect = dbTabPerson.select();

        Assertions.assertEquals(dbSelectPersonWithTwoRowsIdEq2Updated, dbSelect);
    }

/*
    @Test
    public void updateTwoRowsTableButSetHasWrongFieldsAndWhereHasWrongNameFields() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  UPDATE person
        //     SET wrong_field = wrong_field_2 || " " || name
        //       , wrong_field_2 = "   "
        //   WHERE wrong_field = 2
        //      OR wrong_field_2 = "Bob"
        DbSQLException exception = Assertions.assertThrows(
                DbSQLException.class,
                //  Не получится собрать текст для исключения, в котором можно было-бы описать все ошибки и в where и в set.
                //  В этом месте сработает одно исключение на первой части where:
                //      dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD).equals(2)
                //  Если его закомментировать, возникнет исключение на второй части where:
                //      dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD_2).equals("Bob")
                //  Если-же вообще без where,
                //      то исключение возникнет при попытке взять значение несуществующего поля перед тем, как вызовется Map.of
                //          dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD_2)
                //      если-же значения для мапы будут рассчитаны, мапа создастся, но ошибка в имени будет найдена в ключах мапы,
                //      тогда исключение возникнет в setAll.
                //          DB_FIELD_NAME_WRONG_FIELD.
                () -> dbTabPerson.update(
                        dbRec -> Map.of(
                                DB_FIELD_NAME_WRONG_FIELD, dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD_2) + " " + dbRec.getValue(DB_FIELD_NAME_NAME),
                                DB_FIELD_NAME_WRONG_FIELD_2, "   "
                        ),
                        dbRec ->
                                dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD).equals(2) ||
                                        dbRec.getValue(DB_FIELD_NAME_WRONG_FIELD_2).equals("Bob")
                )
        );

        Assertions.assertEquals(
                String.format(ERROR_COLUMN_DOESNT_EXIST, DB_FIELD_NAME_WRONG_FIELD),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }
*/
}
