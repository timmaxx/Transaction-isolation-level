package com.timmax.training_demo.transaction_isolation_level.v02;

public class DbFieldDefinition<T> {
    private final Class<T> clazz;
    private boolean nullable = true;

    public DbFieldDefinition(Class<T> clazz) {
        this.clazz = clazz;
    }

    public DbFieldDefinition(Class<T> clazz, boolean nullable) {
        this(clazz);
        this.nullable = nullable;
    }

    public Class<T> getClazz() {
        return clazz;
    }

    public boolean isNullable() {
        return nullable;
    }
}
