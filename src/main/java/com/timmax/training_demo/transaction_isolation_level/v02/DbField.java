package com.timmax.training_demo.transaction_isolation_level.v02;

public class DbField {
    private final DbFieldName dbFieldName;
    private final Class<?> type;

    public DbField(String name, Class<?> type) {
        this.dbFieldName = new DbFieldName(name);
        this.type = type;
    }

    public DbField(DbFieldName dbFieldName, Class<?> type) {
        this(dbFieldName.getName(), type);
    }

    public DbFieldName getDbFieldName() {
        return dbFieldName;
    }

    public Class<?> getType() {
        return type;
    }
}
