package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;

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
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPersonEmpty.getDMLCommandUpdate(
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
    public void updateReadOnlyTableWithUpdateSetCalcFuncIsNull() {
        //  UPDATE person   --  0 rows and table is read only
        //     SET -- updateSetCalcFunc is null - WRONG SYNTAX OF UPDATE
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPersonEmpty.getDMLCommandUpdate(null)
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

    private void updateTwoRowsTable(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  UPDATE person   --  2 rows
        //     SET name = name || " " || name
        sqlCommandQueue1.add(
                dbTabPerson.getDMLCommandUpdate(
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        )
                ),
                dbTabPerson.getDQLCommandSelect()
        );

        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRowsAllUpdated, dbSelect);
    }

    @Test
    public void updateTwoRowsTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        updateTwoRowsTable(dbTabPerson, sqlCommandQueue1);
    }

    private void updateTwoRowsTableWhereIdEq2(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  UPDATE person   --  2 rows
        //     SET name = name || " " || name
        //   WHERE id = 2
        sqlCommandQueue1.add(
                dbTabPerson.getDMLCommandUpdate(
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        ),
                        dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(2)
                ),
                dbTabPerson.getDQLCommandSelect()
        );

        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRowsIdEq2Updated, dbSelect);
    }

    @Test
    public void updateTwoRowsTableWhereIdEq2() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        updateTwoRowsTableWhereIdEq2(dbTabPerson, sqlCommandQueue1);
    }

    @Test
    public void updateTwoRowsTableAndRollback() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        updateTwoRowsTable(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRows, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void updateTwoRowsTableAndCommit() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        updateTwoRowsTable(dbTabPerson, sqlCommandQueue1);

        //  COMMIT;
        sqlCommandQueue1.commit();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRowsAllUpdated, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void updateTwoRowsTableWhereIdEq2AndRollback() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        updateTwoRowsTableWhereIdEq2(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRows, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void updateTwoRowsTableWhereIdEq2AndCommit() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        updateTwoRowsTableWhereIdEq2(dbTabPerson, sqlCommandQueue1);

        //  COMMIT;
        sqlCommandQueue1.commit();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRowsIdEq2Updated, sqlCommandQueue1, dbTabPerson);
    }
}
