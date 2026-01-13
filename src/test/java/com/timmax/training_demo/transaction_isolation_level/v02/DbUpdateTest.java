package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
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

    DbTab dbTabPersonWithTwoRows;
    SQLCommandQueue sqlCommandQueue;


    @BeforeEach
    public void beforeEach() {
        dbTabPersonWithTwoRows = new DbTab(dbTabPersonRoWithTwoRows, false);
        sqlCommandQueue = new SQLCommandQueue();
    }

    @Test
    public void updateReadOnlyTableViaMainThread() {
        //  UPDATE person   --  0 rows and table is read only
        //     SET name = name || " " || name
        sqlCommandQueue.add(
                dbTabPersonRoEmpty.getDMLCommandUpdate(
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        )
                )
        );

        sqlCommandQueue.startThread();
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                sqlCommandQueue::joinToThread
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
        sqlCommandQueue.add(
                dbTabPersonRoEmpty.getDMLCommandUpdate(null)
        );

        sqlCommandQueue.startThread();
        NullPointerException exception = Assertions.assertThrows(
                NullPointerException.class,
                sqlCommandQueue::joinToThread
        );

        assertEquals(
                String.format(ERROR_UPDATE_SET_CALC_FUNC_IS_NULL_BUT_YOU_CANNOT_MAKE_IT_NULL),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    private void updateTwoRowsTable(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue) {
        //  UPDATE person   --  2 rows
        //     SET name = name || " " || name
        sqlCommandQueue.add(
                dbTabPerson.getDMLCommandUpdate(
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        )
                ),
                dbTabPerson.getDQLCommandSelect()
        );

        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        DbSelect dbSelect = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRowsAllUpdated, dbSelect);
    }

    @Test
    public void updateTwoRowsTable() {
        DbTab dbTabPerson = dbTabPersonWithTwoRows;

        updateTwoRowsTable(dbTabPerson, sqlCommandQueue);
    }

    private void updateTwoRowsTableWhereIdEq2(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue) {
        //  UPDATE person   --  2 rows
        //     SET name = name || " " || name
        //   WHERE id = 2
        sqlCommandQueue.add(
                dbTabPerson.getDMLCommandUpdate(
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        ),
                        dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(2)
                ),
                dbTabPerson.getDQLCommandSelect()
        );

        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        DbSelect dbSelect = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRowsIdEq2Updated, dbSelect);
    }

    @Test
    public void updateTwoRowsTableWhereIdEq2() {
        DbTab dbTabPerson = dbTabPersonWithTwoRows;

        updateTwoRowsTableWhereIdEq2(dbTabPerson, sqlCommandQueue);
    }

    @Test
    public void updateTwoRowsTableAndRollback() {
        DbTab dbTabPerson = dbTabPersonWithTwoRows;

        updateTwoRowsTable(dbTabPerson, sqlCommandQueue);

        //  ROLLBACK;
        sqlCommandQueue.rollback();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRows, sqlCommandQueue, dbTabPerson);
    }

    @Test
    public void updateTwoRowsTableAndCommit() {
        DbTab dbTabPerson = dbTabPersonWithTwoRows;

        updateTwoRowsTable(dbTabPerson, sqlCommandQueue);

        //  COMMIT;
        sqlCommandQueue.commit();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRowsAllUpdated, sqlCommandQueue, dbTabPerson);
    }

    @Test
    public void updateTwoRowsTableWhereIdEq2AndRollback() {
        DbTab dbTabPerson = dbTabPersonWithTwoRows;

        updateTwoRowsTableWhereIdEq2(dbTabPerson, sqlCommandQueue);

        //  ROLLBACK;
        sqlCommandQueue.rollback();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRows, sqlCommandQueue, dbTabPerson);
    }

    @Test
    public void updateTwoRowsTableWhereIdEq2AndCommit() {
        DbTab dbTabPerson = dbTabPersonWithTwoRows;

        updateTwoRowsTableWhereIdEq2(dbTabPerson, sqlCommandQueue);

        //  COMMIT;
        sqlCommandQueue.commit();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRowsIdEq2Updated, sqlCommandQueue, dbTabPerson);
    }
}
