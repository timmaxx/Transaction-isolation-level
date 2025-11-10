package com.timmax.training_demo.transaction_isolation_level.v02;

public class DbFieldDefinition<T> {
    private final Class<T> clazz;

    public DbFieldDefinition(Class<T> clazz) {
        this.clazz = clazz;
    }

    public Class<T> getClazz() {
        return clazz;
    }
}
