package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.HashSet;
import java.util.Set;

public class DbTab {
    private final String Name;
    private final  DbFields dbFields;
    private final Set<DbRec> dbRecs = new HashSet<>();

    public DbTab(String name, DbFields dbFields) {
        Name = name;
        this.dbFields = dbFields;
    }

    void insert(DbFieldNames dbFieldNames, DbRec dbRec) {
        if (dbFieldNames.size() != dbRec.size()) {
            throw new RuntimeException("dbFieldNames.size() != values.length");
        }
        dbRecs.add(dbRec);
    }

    @Override
    public String toString() {
        return "DbTab{" +
                "Name='" + Name + '\'' +
                ", dbFields=" + dbFields +
                ", dbRecs=" + dbRecs +
                '}';
    }
}
