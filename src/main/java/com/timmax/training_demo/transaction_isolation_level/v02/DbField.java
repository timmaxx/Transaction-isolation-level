package com.timmax.training_demo.transaction_isolation_level.v02;

public class DbField<T> {
    private final DbFieldName dbFieldName;
    private final Class<T> type;

    public DbField(String name, Class<T> type) {
        this.dbFieldName = new DbFieldName(name);
        this.type = type;
    }

    public DbField(DbFieldName dbFieldName, Class<T> type) {
        this(dbFieldName.getName(), type);
    }

    public DbFieldName getDbFieldName() {
        return dbFieldName;
    }

    public Class<T> getType() {
        return type;
    }
}
