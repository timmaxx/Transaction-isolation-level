package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class DbFieldValue {
    protected static final Logger logger = LoggerFactory.getLogger(DbFieldValue.class);

    public static final String ERROR_VALUE_IS_WRONG_TYPE = "ERROR: Value (%s) is wrong type (right type is %s).";

    private final Class<?> clazz;
    private Object value;

    public DbFieldValue(Class<?> clazz, Object value) {
        this.clazz = clazz;
        setValue(/*(clazz)*/value);
    }

    public void setValue(Object value) {
        if (value != null) {
            verifyClassOfNewOrComparableValue(value);
        }
        this.value = value;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        verifyClassOfNewOrComparableValue(o);
        return value.equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clazz, value);
    }

    private void verifyClassOfNewOrComparableValue(Object o) {
        if (!clazz.equals(o.getClass())) {
            throw new DbSQLException(String.format(ERROR_VALUE_IS_WRONG_TYPE, o, clazz));
        }
    }
}
