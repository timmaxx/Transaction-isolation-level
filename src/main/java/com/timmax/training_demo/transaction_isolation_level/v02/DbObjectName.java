package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.Objects;

public abstract sealed class DbObjectName
        permits DbTabName, DbFieldName {
    private final String name;

    public DbObjectName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "DbObjectName{" +
                "name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DbObjectName that)) return false;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }
}
