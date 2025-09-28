package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class DbFieldNames {
    private final Set<String> names;

    public DbFieldNames(String... names) {
        this.names = new HashSet<>(Arrays.asList(names));
    }

    public int size() {
        return names.size();
    }

    @Override
    public String toString() {
        return "DbFieldNames{" +
                "names=" + names +
                '}';
    }
}
