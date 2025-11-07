package com.timmax.training_demo.transaction_isolation_level.v02;

public class DbField<T> {
    private final DbFieldName dbFieldName;
    private final Class<T> clazz;

    public DbField(String name, Class<T> clazz) {
        this.dbFieldName = new DbFieldName(name);
        this.clazz = clazz;
    }

    public DbField(DbFieldName dbFieldName, Class<T> clazz) {
        this(dbFieldName.getName(), clazz);
    }

    public DbFieldName getDbFieldName() {
        return dbFieldName;
    }

    public Class<T> getClazz() {
        return clazz;
    }
}
