package com.timmax.training_demo.transaction_isolation_level.v02;

public class DbField {
    private final String name;
    private final Class<?> type;

    public DbField(String name, Class<?> type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Class<?> getType() {
        return type;
    }
}
