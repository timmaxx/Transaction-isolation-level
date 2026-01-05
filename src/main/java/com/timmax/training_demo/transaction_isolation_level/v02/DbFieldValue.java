package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class DbFieldValue {
    protected static final Logger logger = LoggerFactory.getLogger(DbFieldValue.class);

    public static final String ERROR_VALUE_IS_WRONG_TYPE = "ERROR: Value (%s) is wrong type (right type is %s).";
    public static final String ERROR_THIS_METHOD_EQUALS_IS_NOT_ALLOWED_FOR_THIS_CLASS = "ERROR: This method (equals) is not allowed for this class.";

    private final Class<?> clazz;
    private Object value;

    public DbFieldValue(Class<?> clazz, Object value) {
        this.clazz = clazz;
        setValue(value);
    }

    public void setValue(Object value) {
        if (value != null) {
            verifyClassOfNewOrComparableValue(value);
        }
        this.value = value;
    }

    public boolean eq(Object o) {
        if (o == null) {
            return false;
        }
        verifyClassOfNewOrComparableValue(o);
        return value.equals(o);
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException(ERROR_THIS_METHOD_EQUALS_IS_NOT_ALLOWED_FOR_THIS_CLASS);
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
