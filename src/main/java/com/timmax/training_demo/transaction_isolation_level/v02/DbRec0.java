package com.timmax.training_demo.transaction_isolation_level.v02;

public class DbRec0 extends DbRec {
    private final DbTableLike dbTableLike;

    public DbRec0(DbRec dbRec, DbTableLike dbTableLike) {
        super(dbRec);
        this.dbTableLike = dbTableLike;
    }

    public DbRec getSuper() {
        return this;
    }

    public DbTableLike getDbTableLike() {
        return dbTableLike;
    }
}
