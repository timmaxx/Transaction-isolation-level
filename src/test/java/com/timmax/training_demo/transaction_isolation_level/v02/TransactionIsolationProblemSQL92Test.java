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

//  От Isolation к Consistency — дорога длиной в 30 лет
//  https://habr.com/ru/articles/705332/

//  A Critique of ANSI SQL Isolation Levels
//  https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf

/*
    Histories consisting of reads, writes, commits, and aborts can be written in a shorthand notation:
    - “w1[x]” means a write by transaction 1 on data item x (which is how a data item is “modified’),
    - and “r2[x]” represents a read of x by transaction 2.
    - Transaction 1 reading and writing a set of records satisfying predicate P is denoted by r1[P] and w1[P] respectively.
    Transaction 1’s commit and abort (ROLLBACK) are written “c1” and “a1”, respectively.

    Примеры для операций в транзакции 1:
    - w1[x] INSERT, UPDATE, DELETE в элемент данных x,
    - r1[x] SELECT элемента данных x,
    - w1[P] INSERT, UPDATE, DELETE для множества записей, удовлетворяющих предикату P,
    - r1[P] SELECT для множества записей, удовлетворяющих предикату P,
    - c1    COMMIT,
    - a1    ROLLBACK.
*/


public class TransactionIsolationProblemSQL92Test {
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


    //  P1. Dirty read - Грязное чтение
    //      A1: w1[x]...r2[x]...(a1 and c2 in either order)
    //      Критики утверждают, что на вопрос надо смотреть шире:
    //      P1: w1[x]...r2[x]...(c1 or a1)
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

    //  P2. Non-repeatable or Fuzzy Read - Неповторяемое или нечеткое чтение
    //      A2: r1[x]...w2[x]...c2...r1[x]...c1
    //      Критики утверждают, что на вопрос надо смотреть шире:
    //      P2: r1[x]...w2[x]...(c1 or a1)
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

    //  P3. Phantom - Фантом
    //      (это похоже на "Неповторяемое или нечеткое чтение",
    //      но вторая транзакция может изменить количество записей в таблице - т.е. может возникнуть фантом)
    //      A3: r1[P]...w2[y in P]...c2...r1[P]...c1
    //      Критики утверждают, что на вопрос надо смотреть шире:
    //      P3: r1[P]...w2[y in P]...(c1 or a1)
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
