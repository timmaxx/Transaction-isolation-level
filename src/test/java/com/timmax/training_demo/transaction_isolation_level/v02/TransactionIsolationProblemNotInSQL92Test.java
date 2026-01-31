package com.timmax.training_demo.transaction_isolation_level.v02;

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

    - rc    Подразумевается операция чтения через курсор.
*/


public class TransactionIsolationProblemNotInSQL92Test {
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


    //  P0. Dirty write - Грязная запись
    //      P0: w1[x]...w2[x]...((c1 or a1) and (c2 or a2) in any order)
    //      Может привести к аномалии:
    //      w1[x=1]...w2[x=2]...w2[y=2]...c2...w1[y=1]...c1
    //      На базу наложено ограничение целостности x == y
    //      Первая транзакция записывает в x и y значение 1, вторая — значение 2
    //      В результате x == 2, y == 1, что нехорошо
    //      Авторы считают, что любой уровень изоляции из SQL-92 должен исключать Dirty Write
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

    //  P4. Lost update - Потерянное обновление
    //      В стандарте отсутствует, предлагается такая формулировка:
    //      P4: r1[x=1]...w2[x=10]...w1[x=1+1]...c1
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

/*
    //  1.2.  Lost update - Потерянное обновление (с предварительным SELECT)
    //  Описан здесь:
    //  https://habr.com/ru/companies/infopulse/articles/261097/
    //  Для реализации теста такого сценария нужно сделать SELECT перед каждым UPDATE
    //  и что-то из SELECT использовать в UPDATE.
    //  Сценарий будет демонстрировать, что и при SELECT нужно делать блокировку записей для лучшей изолированности
    //  транзакций.
    @Test
    public void lostUpdateProblem2() {
    }
*/

    //  P4C. Cursor Lost Update - Курсорное потерянное обновление
    //      В стандарте отсутствует, предлагается такая формулировка:
    //      P4C: rc1[x=1]...w2[x=10]...w1[x=1+1]...c1
    //      Здесь под rc подразумевается операция чтения через курсор,
    //      смысл феномена — запретить менять переменную, на которой находится курсор


    //  A5A (Read Skew)
    //      В стандарте отсутствует, предлагается такая формулировка:
    //      A5A: r1[x]...w2[x]...w2[y]...c2...r1[y]...(c1 or a1)


    //  A5B (Write Skew)
    //      В стандарте отсутствует, предлагается такая формулировка:
    //      A5B: r1[x]...r2[y]...w1[y]...w2[x]...(c1 and c2 occur)
}
