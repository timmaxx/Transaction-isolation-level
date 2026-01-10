package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.SQLCommandQueue;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandUpdate;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql.DQLCommandSelect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTab.ERROR_TABLE_IS_RO_YOU_CANNOT_UPDATE;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTab.ERROR_UPDATE_SET_CALC_FUNC_IS_NULL_BUT_YOU_CANNOT_MAKE_IT_NULL;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DbUpdateTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbUpdateTest.class);

    @Test
    public void updateReadOnlyTableViaMainThread() {
        //  UPDATE person   --  0 rows and table is read only
        //     SET name = name || " " || name
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                () -> dbTabPersonEmpty.update(
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        )
                )
        );

        assertEquals(
                String.format(ERROR_TABLE_IS_RO_YOU_CANNOT_UPDATE, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void updateReadOnlyTableViaMainThreadViaSQLCommandQueue() {
        //  UPDATE person   --  0 rows and table is read only
        //     SET name = name || " " || name
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DMLCommandUpdate(
                        dbTabPersonEmpty,
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        )
                )
        );

        sqlCommandQueue1.startThread();
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                sqlCommandQueue1::joinToThread
        );

        assertEquals(
                String.format(ERROR_TABLE_IS_RO_YOU_CANNOT_UPDATE, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void updateReadOnlyTableWithUpdateSetCalcFuncIsNullViaMainThread() {
        //  UPDATE person   --  0 rows and table is read only
        //     SET -- updateSetCalcFunc is null - WRONG SYNTAX OF UPDATE
        NullPointerException exception = Assertions.assertThrows(
                NullPointerException.class,
                () -> dbTabPersonEmpty.update(null)
        );

        assertEquals(
                String.format(ERROR_UPDATE_SET_CALC_FUNC_IS_NULL_BUT_YOU_CANNOT_MAKE_IT_NULL),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void updateReadOnlyTableWithUpdateSetCalcFuncIsNullViaSQLCommandQueue() {
        //  UPDATE person   --  0 rows and table is read only
        //     SET -- updateSetCalcFunc is null - WRONG SYNTAX OF UPDATE
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DMLCommandUpdate(
                        dbTabPersonEmpty,
                        null
                )
        );

        sqlCommandQueue1.startThread();
        NullPointerException exception = Assertions.assertThrows(
                NullPointerException.class,
                sqlCommandQueue1::joinToThread
        );

        assertEquals(
                String.format(ERROR_UPDATE_SET_CALC_FUNC_IS_NULL_BUT_YOU_CANNOT_MAKE_IT_NULL),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void updateTwoRowsTableViaMainThread() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  UPDATE person   --  2 rows
        //     SET name = name || " " || name
        dbTabPerson.update(
                dbRec -> Map.of(
                        DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                )
        );

        DbSelect dbSelect = dbTabPerson.select().getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRowsAllUpdated, dbSelect);
    }

    private void updateTwoRowsTableViaSQLCommandQueue(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  UPDATE person   --  2 rows
        //     SET name = name || " " || name
        sqlCommandQueue1.add(
                new DMLCommandUpdate(
                        dbTabPerson,
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        )
                ),
                new DQLCommandSelect(dbTabPerson)
        );

        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRowsAllUpdated, dbSelect);
    }

    @Test
    public void updateTwoRowsTableViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        updateTwoRowsTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);
    }

    @Test
    public void updateTwoRowsTableWhereIdEq2ViaMainThread() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  UPDATE person   --  2 rows
        //     SET name = name || " " || name
        //   WHERE id = 2
        dbTabPerson.update(
                dbRec -> Map.of(
                        DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                ),
                dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(2)
        );

        DbSelect dbSelect = dbTabPerson.select().getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRowsIdEq2Updated, dbSelect);
    }

    private void updateTwoRowsTableWhereIdEq2ViaSQLCommandQueue(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  UPDATE person   --  2 rows
        //     SET name = name || " " || name
        //   WHERE id = 2
        sqlCommandQueue1.add(
                new DMLCommandUpdate(
                        dbTabPerson,
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        ),
                        dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(2)
                ),
                new DQLCommandSelect(dbTabPerson)
        );

        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRowsIdEq2Updated, dbSelect);
    }

    @Test
    public void updateTwoRowsTableWhereIdEq2ViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        updateTwoRowsTableWhereIdEq2ViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);
    }

    @Test
    public void updateTwoRowsTableViaSQLCommandQueueAndRollBack() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        updateTwoRowsTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();
        //  Смущает, что селект после отката сделал не через SQLCommandQueue:
        DbSelect dbSelect2 = dbTabPerson.select().getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRows, dbSelect2);
    }

    @Test
    public void updateTwoRowsTableWhereIdEq2ViaSQLCommandQueueAndRollBack() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        updateTwoRowsTableWhereIdEq2ViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();
        //  Смущает, что селект после отката сделал не через SQLCommandQueue:
        DbSelect dbSelect2 = dbTabPerson.select().getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRows, dbSelect2);
    }
}
