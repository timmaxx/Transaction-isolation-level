package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.SQLCommandQueue;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql.DQLCommandSelect;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbSelectTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbSelectTest.class);

    //  ToDo:   Создать библиотечный метод, который принимает две выборки и сравнивает их.
    //          В тестах, сейчас применяется сравнение выборок по двум вариантам:
    //          1.  Где количество строк в выборках, которые сравниваются, 0 или 1 строк.
    //              А значит сортировать не нужно и сравнение вообще примитивное.
    //          2.  Где количество строк 2 и более и они могут быть по разному отсортированы.
    //              Приходится сортировать списки java и потом их сравнивать.
    //          Правильней сравнивать результаты выборок так, как это принято в SQL.
    //          И в т.ч. нужно допускать в выборках наличие дубликатов.
    //          Пусть у нас есть две таблицы (table1 и table2) с одинаковым набором полей (id, name, email).
    //          Тогда сначала так:
    //              SELECT  COUNT(*) count
    //                   ,  id, name, email
    //                FROM  table1
    //               GROUP BY id, name, email
    //              EXCEPT  --  Для Oracle - MINUS
    //              SELECT  COUNT(*) count
    //                   ,  id, name, email
    //                FROM  table2
    //               GROUP BY id, name, email
    //          Должно получиться 0 строк.
    //          А потом в обратную сторону. И тоже должно получиться 0 строк.

    @Test
    public void selectFromEmptyTableViaMainThread() {
        //  SELECT *
        //    FROM person   --  0 rows
        DbSelect dbSelect = dbTabPersonEmpty.select().getDbSelect();

        //  ToDo:   Переделать. Здесь пример примитивного сравнения двух выборок.
        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void selectFromEmptyTableViaSQLCommandQueue() {
        //  SELECT *
        //    FROM person   --  0 rows
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DQLCommandSelect(dbTabPersonEmpty)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        //  ToDo:   Переделать. Здесь пример примитивного сравнения двух выборок.
        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void selectFromOneRowTableViaMainThread() {
        //  SELECT *
        //    FROM person   --  1 row (dbRec1_Bob_email)
        DbSelect dbSelect = dbTabPersonWithOneRow.select().getDbSelect();

        //  ToDo:   Переделать. Здесь пример примитивного сравнения двух выборок.
        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void selectFromOneRowTableViaSQLCommandQueue() {
        //  SELECT *
        //    FROM person   --  1 row (dbRec1_Bob_email)
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DQLCommandSelect(dbTabPersonWithOneRow)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        //  ToDo:   Переделать. Здесь пример примитивного сравнения двух выборок.
        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void selectFromTwoRowsTableViaMainThread() {
        //  SELECT *
        //    FROM person   --  2 rows (dbRec1_Bob_email, dbRec2_Alice_email)
        DbSelect dbSelect = dbTabPersonWithTwoRows.select().getDbSelect();

        //  ToDo:   Переделать. Здесь пример примитивного сравнения двух выборок.
        Assertions.assertEquals(dbSelectPersonWithTwoRows, dbSelect);
    }

    @Test
    public void selectFromTwoRowsTableViaSQLCommandQueue() {
        //  SELECT *
        //    FROM person   --  2 rows (dbRec1_Bob_email, dbRec2_Alice_email)
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DQLCommandSelect(dbTabPersonWithTwoRows)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        //  ToDo:   Переделать. Здесь пример примитивного сравнения двух выборок.
        Assertions.assertEquals(dbSelectPersonWithTwoRows, dbSelect);
    }

    @Test
    public void selectFromEmptyTableWhereIdEq1ViaMainThread() {
        //  SELECT *
        //    FROM person   --  0 rows
        //   WHERE id = 1
        DbSelect dbSelect = dbTabPersonEmpty.select(
                dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(1)
        ).getDbSelect();

        //  ToDo:   Переделать. Здесь пример примитивного сравнения двух выборок.
        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void selectFromEmptyTableWhereIdEq1ViaSQLCommandQueue() {
        //  SELECT *
        //    FROM person   --  0 rows
        //   WHERE id = 1
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DQLCommandSelect(dbTabPersonEmpty, dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(1))
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        //  ToDo:   Переделать. Здесь пример примитивного сравнения двух выборок.
        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void selectFromOneRowTableWhereIdEq1ViaMainThread() {
        //  SELECT *
        //    FROM person   --  1 row (dbRec1_Bob_email)
        //   WHERE id = 1
        DbSelect dbSelect = dbTabPersonWithOneRow.select(
                dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(1)
        ).getDbSelect();

        //  ToDo:   Переделать. Здесь пример примитивного сравнения двух выборок.
        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void selectFromOneRowTableWhereIdEq1ViaSQLCommandQueue() {
        //  SELECT *
        //    FROM person   --  1 row (dbRec1_Bob_email)
        //   WHERE id = 1
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DQLCommandSelect(dbTabPersonWithOneRow, dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(1))
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        //  ToDo:   Переделать. Здесь пример примитивного сравнения двух выборок.
        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void selectFromTwoRowsTableWhereIdEq1ViaMainThread() {
        //  SELECT *
        //    FROM person   --  2 rows (dbRec1_Bob_email, dbRec2_Alice_email)
        //   WHERE id = 1
        DbSelect dbSelect = dbTabPersonWithTwoRows.select(
                dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(1)
        ).getDbSelect();

        //  ToDo:   Переделать. Здесь пример примитивного сравнения двух выборок.
        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void selectFromTwoRowsTableWhereIdEq1ViaSQLCommandQueue() {
        //  SELECT *
        //    FROM person   --  2 rows (dbRec1_Bob_email, dbRec2_Alice_email)
        //   WHERE id = 1
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DQLCommandSelect(dbTabPersonWithTwoRows, dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(1))
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        //  ToDo:   Переделать. Здесь пример примитивного сравнения двух выборок.
        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }
}
