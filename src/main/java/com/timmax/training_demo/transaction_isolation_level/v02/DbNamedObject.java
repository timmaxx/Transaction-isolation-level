package com.timmax.training_demo.transaction_isolation_level.v02;

public class DbNamedObject {
    private final String Name;

    public DbNamedObject(String name) {
        Name = name;
    }

    public String getName() {
        return Name;
    }
}
