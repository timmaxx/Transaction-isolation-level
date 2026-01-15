package com.timmax.training_demo.transaction_isolation_level.v02;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

//  Уровень изолированности транзакций
//  Проблемы параллельного доступа с использованием транзакций
//  https://ru.wikipedia.org/wiki/%D0%A3%D1%80%D0%BE%D0%B2%D0%B5%D0%BD%D1%8C_%D0%B8%D0%B7%D0%BE%D0%BB%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%BD%D0%BE%D1%81%D1%82%D0%B8_%D1%82%D1%80%D0%B0%D0%BD%D0%B7%D0%B0%D0%BA%D1%86%D0%B8%D0%B9

public class TransactionIsolationProblemTest {
    DbTab dbTabPersonEmpty;
    DbTab dbTabPersonWithOneRow;
    SQLCommandQueue sqlCommandQueue1;
    SQLCommandQueue sqlCommandQueue2;


    @BeforeEach
    public void beforeEach() {
        dbTabPersonEmpty = new DbTab(dbTabPersonRoEmpty, false);
        dbTabPersonWithOneRow = new DbTab(dbTabPersonRoWithOneRow, false);
        sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue2 = new SQLCommandQueue();
    }

    //  1.  Lost update - Потерянное обновление
    @Test
    public void lostUpdateProblem() {
        DbTab dbTabPerson = dbTabPersonWithOneRow;

        //  --  Transaction 1:                      |   --  Transaction 2:
        //  UPDATE person   --  1 row               |   UPDATE person   --  1 row
        //     SET name = name || " " || name;      |      SET name = name || " " || name;
        //                                          |   SELECT *
        //                                          |     FROM person;  --  1 row

        sqlCommandQueue1.add(
                //  Пауза внутри update нужна для демонстрации lostUpdateProblem, но не для других проблем.
                //  И она должна быть больше, чем пауза перед стартом update в другой транзакции.
                dbTabPerson.getDMLCommandUpdate(
                        0L, 100L,
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        )
                )
        );

        sqlCommandQueue2.add(
                dbTabPerson.getDMLCommandUpdate(
                        10L, 100L,
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        )
                ),
                dbTabPerson.getDQLCommandSelect()
        );

        sqlCommandQueue1.startThread();
        sqlCommandQueue2.startThread();
        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();

        DbSelect dbSelect = sqlCommandQueue2.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow_BobBob, dbSelect);
    }

    //  2.  Dirty read - Грязное чтение
    @Test
    public void dirtyReadProblem() {
        DbTab dbTabPerson = dbTabPersonWithOneRow;

        //  --  Transaction 1:                      |   --  Transaction 2:
        //  UPDATE person   --  1 row               |
        //     SET name = name || " " || name;      |
        //                                          |   SELECT *
        //                                          |     FROM person;  --  1 row
        //  ROLLBACK;                               |
        //  SELECT *                                |
        //    FROM person;  --  1 row               |

        sqlCommandQueue1.add(
                dbTabPerson.getDMLCommandUpdate(
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        )
                )
        );

        sqlCommandQueue2.add(
                dbTabPerson.getDQLCommandSelect(200L)
        );

        sqlCommandQueue1.startThread();
        sqlCommandQueue2.startThread();
        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();

        DbSelect dbSelect1 = sqlCommandQueue2.popFromDQLResultLog();

        sqlCommandQueue1.rollback();

        sqlCommandQueue1.add(
                dbTabPerson.getDQLCommandSelect()
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect2 = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertNotEquals(dbSelect2, dbSelect1);
    }

    //  3.  Non-repeatable read - Неповторяющееся чтение
    @Test
    public void NonRepeatableReadProblem() {
        DbTab dbTabPerson = dbTabPersonWithOneRow;

        //  --  Transaction 1:                      |   --  Transaction 2:
        //  SELECT *                                |
        //    FROM person; --  1 row                |
        //                                          |   UPDATE person   --  1 row
        //                                          |      SET name = name || " " || name;
        //                                          |   COMMIT;
        //  SELECT *                                |
        //    FROM person; --  1 row                |

        sqlCommandQueue1.add(
                dbTabPerson.getDQLCommandSelect()
        );

        sqlCommandQueue2.add(
                dbTabPerson.getDMLCommandUpdate(
                        100L, 0L,
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        )
                )
        );

        sqlCommandQueue1.startThread();
        sqlCommandQueue2.startThread();
        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();

        DbSelect dbSelect1 = sqlCommandQueue1.popFromDQLResultLog();

        sqlCommandQueue2.commit();

        sqlCommandQueue1.add(
                dbTabPerson.getDQLCommandSelect()
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect2 = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertNotEquals(dbSelect1, dbSelect2);
    }

    //  4.  Phantom read - Фантомное чтение
    @Test
    public void PhantomReadProblem() {
        DbTab dbTabPerson = dbTabPersonEmpty;

        //  --  Transaction 1:                      |   --  Transaction 2:
        //  SELECT COUNT(*)                         |
        //    FROM person;  --  count = 0           |
        //                                          |   INSERT
        //                                          |     INTO person  --  0 row to 1 row
        //                                          |          (id, name, email)
        //                                          |   VALUES (1, "Bob", "@");
        //                                          |   COMMIT;
        //  SELECT COUNT(*)                         |
        //    FROM person;  --  count = 1           |

        sqlCommandQueue1.add(
                dbTabPerson.getDQLCommandSelect()
        );

        sqlCommandQueue2.add(
                dbTabPerson.getDMLCommandInsert(100L, dbRec1_Bob_email)
        );

        sqlCommandQueue1.startThread();
        sqlCommandQueue2.startThread();
        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();

        sqlCommandQueue2.commit();

        DbSelect dbSelect1 = sqlCommandQueue1.popFromDQLResultLog();

        sqlCommandQueue1.add(
                dbTabPerson.getDQLCommandSelect()
        );

        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect2 = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertNotEquals(dbSelect1.count(), dbSelect2.count());
    }
}
