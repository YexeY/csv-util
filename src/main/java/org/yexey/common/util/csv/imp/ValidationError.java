package org.yexey.common.util.csv.imp;

public class ValidationError {
    private final org.yexey.common.util.csv.imp.Record record;
    private final String message;

    public ValidationError(org.yexey.common.util.csv.imp.Record record, String message) {
        this.record = record;
        this.message = message;
    }

    public Record getRecord() {
        return record;
    }

    public String getMessage() {
        return message;
    }
}