package com.timmax.training_demo.transaction_isolation_level.v02;

import org.junit.jupiter.api.Assertions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class DbSelectUtil {
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


    //  Это реализация не соответствует той, которая описана в ToDo.
    //  Это то, что указано как вариант 2.
    //  Он рабочий, но для его работы требуется доступ к строкам выборок (ч/з getRows()).
    //  Сейчас getRows() объявлен как protected, а следовательно тесты должны быть в том же пакете, что и DbSelect.
    //  Ну и при правильной реализации getRows() можно будет удалить.
    static public void assertEquals(DbSelect dbSelectExpected, DbSelect dbSelectActual) {
        List<DbRec> expectedList = new ArrayList<>(dbSelectExpected.getRows());
        List<DbRec> actualList = new ArrayList<>(dbSelectActual.getRows());
        expectedList.sort(Comparator.naturalOrder());
        actualList.sort(Comparator.naturalOrder());

        Assertions.assertEquals(expectedList, actualList);
    }

    static public void selectFromDbTabViaSQLCommandQueueAndAssertEqualsWithExpectedDbSelect(DbSelect expectedDbSelect, SQLCommandQueue sqlCommandQueue, DbTab actualDbTab) {
        //  SELECT *
        //    FROM person
        sqlCommandQueue.add(actualDbTab.getDQLCommandSelect(actualDbTab));
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        DbSelect actualDbSelect = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(expectedDbSelect, actualDbSelect);
    }
}
