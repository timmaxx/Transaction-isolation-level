package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public abstract sealed class DbTableLike permits DbTab, DbSelect {
    public static final String ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE = "ERROR: Duplicate key value violates unique constraint (combinations of all fields must be unique).";

    protected final DbFields dbFields;

    //  Done:
    //  Поскольку в качестве коллекции для хранения строк используется Set,
    //  новый элемент может быть не добавлен, если точно такой уже есть
    //  (причём в Set значения типа null при сравнении с null дают true, в отличии от SQL).
    //  Это нужно проверять:
    //  - при INSERT (см. insert0 в этом классе - реализовано),
    //  - при UPDATE (см. update0 в классе-потомке - реализовано).
    //  ToDo:   Вместо Set использовать, что-то, что позволяет хранить дубликаты.
    //  Для большей схожести с реляционной алгеброй следовало-бы вместо Set использовать что-то,
    //  что позволяет хранить дубликаты, например, List.
    //  Но это более академическая задача, поэтому пока пусть будет Set и будем считать,
    //  что комбинация значений всех полей составляет уникальный ключ.
    protected final Set<DbRec> dbRecs = new HashSet<>();

    public DbTableLike(DbFields dbFields) {
        this.dbFields = dbFields;
    }

    public DbSelect select() {
        return select0(null);
    }

    public DbSelect select(WhereFunc whereFunc) {
        return select0(whereFunc);
    }

    private DbSelect select0(WhereFunc whereFunc) {
        DbSelect dbSelect = new DbSelect(this.dbFields);
        for (DbRec dbRec : dbRecs) {
            if (whereFunc == null || whereFunc.where(dbRec)) {
                dbSelect.insert0(dbRec);
            }
        }
        return dbSelect;
    }

    protected void insert0(DbRec newDbRec) {
        if (!dbRecs.add(new DbRec(newDbRec))) {
            throw new DbSQLException(ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE);
        }
    }

    protected void insert0(Set<DbRec> newDbRecSet) {
        newDbRecSet.forEach(this::insert0);
    }

    @Override
    public String toString() {
        return "DbTableLike{" +
                "dbFields=" + dbFields +
                ", dbRecs=" + dbRecs +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DbTableLike that)) return false;
        return Objects.equals(dbFields, that.dbFields) && Objects.equals(dbRecs, that.dbRecs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbFields, dbRecs);
    }
}
