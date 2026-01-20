package com.timmax.training_demo.transaction_isolation_level.v02;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;
import static com.timmax.training_demo.transaction_isolation_level.v02.SQLCommandQueueUtil.startAllAndJoinToAllThreads;

//  Уровень изолированности транзакций
//  Проблемы параллельного доступа с использованием транзакций
//  https://ru.wikipedia.org/wiki/%D0%A3%D1%80%D0%BE%D0%B2%D0%B5%D0%BD%D1%8C_%D0%B8%D0%B7%D0%BE%D0%BB%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%BD%D0%BE%D1%81%D1%82%D0%B8_%D1%82%D1%80%D0%B0%D0%BD%D0%B7%D0%B0%D0%BA%D1%86%D0%B8%D0%B9

//  A Critique of ANSI SQL Isolation Levels
//  https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf

public class TransactionIsolationProblemTest {
    DbTab dbTabPersonEmpty;
    DbTab dbTabPersonWithOneRow;
    SQLCommandQueue sqlCommandQueue1;
    SQLCommandQueue sqlCommandQueue2;
    DbSelect dbSelect1;
    DbSelect dbSelect2;


    @BeforeEach
    public void beforeEach() {
        dbTabPersonEmpty = new DbTab(dbTabPersonRoEmpty, false);
        dbTabPersonWithOneRow = new DbTab(dbTabPersonRoWithOneRow, false);
        sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue2 = new SQLCommandQueue();
    }

    //  0.  Dirty write - Грязная запись
    @Test
    public void dirtyWriteProblem() {
        DbTab dbTabPerson = dbTabPersonWithOneRow;

        //  --  Transaction 1:                      |   --  Transaction 2:
        //  UPDATE person   --  1 row               |
        //     SET name = name || " " || name;      |
        //                                          |   UPDATE person   --  1 row
        //                                          |      SET name = name || " " || name;
        //  COMMIT; --  or ROLLBACK;                |
        //                                          |  ROLLBACK;
        //                                          |  SELECT *
        //                                          |    FROM person;  --  1 row

        sqlCommandQueue1.add(
                //  Первый UPDATE изменит значение строки.
                //  В журнале отката будет информация о возможном восстановлении до состояния перед первым UPDATE.
                dbTabPerson.getDMLCommandUpdate(
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        )
                )
        );

        sqlCommandQueue2.add(
                //  Второй UPDATE изменит значение строки уже после того, как первый поменял.
                //  В журнале отката будет информация о возможном восстановлении до состояния перед вторым UPDATE.
                dbTabPerson.getDMLCommandUpdate(
                        100L, 0L,
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        )
                )
        );

        startAllAndJoinToAllThreads(sqlCommandQueue1, sqlCommandQueue2);

        //  (Для COMMIT) Стираем журнал отката первой транзакции.
        sqlCommandQueue1.commit();  //  or .rollback();
        //  Откатываем вторую транзакцию, т.е. до состояния начала второго UPDATE,
        //  т.е. как будто первый UPDATE выполнился.
        sqlCommandQueue2.rollback();

        sqlCommandQueue2.add(
                dbTabPerson.getDQLCommandSelect()
        );
        startAllAndJoinToAllThreads(sqlCommandQueue2);

        dbSelect2 = sqlCommandQueue2.popFromDQLResultLog();

        //  Результирующая выборка должна получиться с учетом первого UPDATE.
        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow_BobBob, dbSelect2);
    }

    //  1.  Lost update - Потерянное обновление
    @Test
    public void lostUpdateProblem() {
        DbTab dbTabPerson = dbTabPersonWithOneRow;

        //  --  Transaction 1:                      |   --  Transaction 2:
        //  UPDATE person   --  1 row               |   UPDATE person   --  1 row
        //     SET name = name || " " || name;      |      SET name = name || " (lost update)";
        //                                          |   SELECT *
        //                                          |     FROM person;  --  1 row

        sqlCommandQueue1.add(
                //  Пауза внутри этого UPDATE (100L) нужна для демонстрации lostUpdateProblem, но не для других проблем.
                //  И она должна быть больше, чем пауза перед стартом UPDATE (10L) в другой транзакции.
                //  Т.е. этот UPDATE успеет прочитать старое значение строки, замрёт (на 100L)
                //  (во время этой паузы выполнится второй UPDATE (он и прочитает и изменит значение строки)),
                //  и вычислит новое значение строки, основываясь на том, что было у него при старте.
                //  И первый UPDATE перепишет, то, что уже переписал второй UPDATE.
                dbTabPerson.getDMLCommandUpdate(
                        0L, 100L,
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " " + dbRec.getValue(DB_FIELD_NAME_NAME)
                        )
                )
        );

        sqlCommandQueue2.add(
                //  Этот UPDATE начнётся немного позже (на 10L) чем начался первый UPDATE.
                //  Пауза внутри этого UPDATE не нужна (0L).
                //  Этот UPDATE успеет прочитать старое значение строки, изменит его по своей формуле до того,
                //  как первый UPDATE изменит значение строки по своей формуле.
                dbTabPerson.getDMLCommandUpdate(
                        10L, 0L,
                        dbRec -> Map.of(
                                DB_FIELD_NAME_NAME, dbRec.getValue(DB_FIELD_NAME_NAME) + " (lost update)"
                        )
                )
        );

        startAllAndJoinToAllThreads(sqlCommandQueue1, sqlCommandQueue2);

        sqlCommandQueue2.add(
                dbTabPerson.getDQLCommandSelect()
        );
        startAllAndJoinToAllThreads(sqlCommandQueue2);

        dbSelect1 = sqlCommandQueue2.popFromDQLResultLog();

        //  Результирующая выборка должна получиться с учетом первого UPDATE,
        //  а результат второго UPDATE будет утерян.
        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow_BobBob, dbSelect1);
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

        startAllAndJoinToAllThreads(sqlCommandQueue1, sqlCommandQueue2);

        dbSelect1 = sqlCommandQueue2.popFromDQLResultLog();

        sqlCommandQueue1.rollback();

        sqlCommandQueue1.add(
                dbTabPerson.getDQLCommandSelect()
        );
        startAllAndJoinToAllThreads(sqlCommandQueue1);

        dbSelect2 = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertNotEquals(dbSelect2, dbSelect1);
    }

    //  3.  Non-repeatable or Fuzzy Read - Неповторяемое или нечеткое чтение
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

        startAllAndJoinToAllThreads(sqlCommandQueue1, sqlCommandQueue2);

        dbSelect1 = sqlCommandQueue1.popFromDQLResultLog();

        sqlCommandQueue2.commit();

        sqlCommandQueue1.add(
                dbTabPerson.getDQLCommandSelect()
        );
        startAllAndJoinToAllThreads(sqlCommandQueue1);

        dbSelect2 = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertNotEquals(dbSelect1, dbSelect2);
    }

    //  4.  Phantom read - Фантомное чтение
    //      (это похоже на "Неповторяемое или нечеткое чтение",
    //      но вторая транзакция может изменить количество записей в таблице - т.е. фантомы)
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

        startAllAndJoinToAllThreads(sqlCommandQueue1, sqlCommandQueue2);

        sqlCommandQueue2.commit();

        dbSelect1 = sqlCommandQueue1.popFromDQLResultLog();

        sqlCommandQueue1.add(
                dbTabPerson.getDQLCommandSelect()
        );
        startAllAndJoinToAllThreads(sqlCommandQueue1);

        DbSelect dbSelect2 = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertNotEquals(dbSelect1.count(), dbSelect2.count());
    }
}
