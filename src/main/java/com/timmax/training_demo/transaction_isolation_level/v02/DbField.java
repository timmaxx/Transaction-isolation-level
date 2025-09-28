package com.timmax.training_demo.transaction_isolation_level.v02;

public class DbField extends DbNamedObject {
    private final Class<?> type;

    public DbField(String name, Class<?> type) {
        super(name);
        this.type = type;
    }

    public Class<?> getType() {
        return type;
    }
}
