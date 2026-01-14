package com.timmax.training_demo.transaction_isolation_level.v02;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

//  Уровень изолированности транзакций
//  Проблемы параллельного доступа с использованием транзакций
//  https://ru.wikipedia.org/wiki/%D0%A3%D1%80%D0%BE%D0%B2%D0%B5%D0%BD%D1%8C_%D0%B8%D0%B7%D0%BE%D0%BB%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%BD%D0%BE%D1%81%D1%82%D0%B8_%D1%82%D1%80%D0%B0%D0%BD%D0%B7%D0%B0%D0%BA%D1%86%D0%B8%D0%B9

public class TransactionIsolationProblemTest {
    DbTab dbTabPersonWithTwoRows;
    SQLCommandQueue sqlCommandQueue1;
    SQLCommandQueue sqlCommandQueue2;


    @BeforeEach
    public void beforeEach() {
        dbTabPersonWithTwoRows = new DbTab(dbTabPersonRoWithTwoRows, false);
        sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue2 = new SQLCommandQueue();
    }

    //  1.  Lost update - Потерянное обновление
    @Test
    public void lostUpdateProblem() {
        DbTab dbTabPerson = dbTabPersonWithTwoRows;

        //  UPDATE person   --  2 rows
        //     SET name = name || " " || name
        //   WHERE id = 2
        sqlCommandQueue1.add(
                //  Пауза внутри update сильно нужна для демонстрации lostUpdateProblem, но не для других проблем.
                //  И она должна быть больше, чем пауза перед стартом update в другой транзакции.
                dbTabPerson.getDMLCommandUpdate(
                        0L, 100L,
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        ),
                        dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(2)
                )
        );

        //  UPDATE person   --  2 rows
        //     SET name = name || " " || name
        //   WHERE id = 2
        sqlCommandQueue2.add(
                dbTabPerson.getDMLCommandUpdate(
                        10L, 100L,
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        ),
                        dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(2)
                ),
                dbTabPerson.getDQLCommandSelect()
        );

        sqlCommandQueue1.startThread();
        sqlCommandQueue2.startThread();
        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();

        DbSelect dbSelect = sqlCommandQueue2.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRowsIdEq2Updated, dbSelect);
    }
}
