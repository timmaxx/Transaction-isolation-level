package com.timmax.training_demo.transaction_isolation_level.v02;

public class DbField<T> {
    private final DbFieldName dbFieldName;
    private final DbFieldDefinition<T> dbFieldDefinition;

    public DbField(DbFieldName dbFieldName, DbFieldDefinition<T> dbFieldDefinition) {
        this.dbFieldName = dbFieldName;
        this.dbFieldDefinition = dbFieldDefinition;
    }

    public DbFieldName getDbFieldName() {
        return dbFieldName;
    }

    public DbFieldDefinition<T> getDbFieldDefinition() {
        return dbFieldDefinition;
    }
}
