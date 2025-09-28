package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class DbTab extends DbNamedObject {
    private boolean readOnly = false;
    private final  DbFields dbFields;
    private final Set<DbRec> dbRecs = new HashSet<>();

    public DbTab(String name, DbFields dbFields) {
        super(name);
        this.dbFields = dbFields;
    }

    public void setReadOnly() {
        readOnly = true;
    }

    void insert(DbFieldNames dbFieldNames, DbRec dbRec) {
        if (readOnly) {
            throw new RuntimeException("Read only");
        }
        if (dbFieldNames.size() != dbRec.size()) {
            throw new RuntimeException("dbFieldNames.size() != values.length");
        }
        dbRecs.add(dbRec);
    }

    @Override
    public String toString() {
        return "DbTab{" +
                "name='" + getName() + '\'' +
                ", readOnly=" + readOnly +
                ", dbFields=" + dbFields +
                ", dbRecs=" + dbRecs +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DbTab dbTab))
            return false;
        return Objects.equals(getName(), dbTab.getName()) &&
                Objects.equals(dbFields, dbTab.dbFields) &&
                Objects.equals(dbRecs, dbTab.dbRecs)
                ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), dbFields, dbRecs);
    }
}
