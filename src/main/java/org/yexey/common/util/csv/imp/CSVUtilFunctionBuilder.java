package org.yexey.common.util.csv.imp;

import java.util.function.Function;
import java.util.function.Predicate;

public class CSVUtilFunctionBuilder {

    public static Function<Record, Record> rename(String columnBefore, String columnAfter) {
        return record -> {
            if(!record.containsColumn(columnBefore)) {
                throw new NullPointerException("Column " + columnBefore + " not found");
            }
            return record.rename(columnBefore, columnAfter);
        };
    }

    public static Function<Record, Record> mapColumn(String column, Function<String, String> function) {
        return record -> {
            if(!record.containsColumn(column)) {
                throw new NullPointerException("Column " + column + " not found");
            }
            return record.put(column, function.apply(record.get(column)));
        };
    }

    public static Function<Record, Record> deleteColumns(String... columns) {
        return record -> record.deleteColumns(columns);
    }

    public static Function<Record, Record> retainColumn(String... columns) {
        return record -> record.retainColumns(columns);
    }

    public static Predicate<? super Record> filter(String column, Predicate<String> predicate) {
        return record -> predicate.test(record.get(column));
    }

    public static Function<Record, Record> addColumn(String columnName, Function<Record, String> valueFunction) {
        return record -> record.put(columnName, valueFunction.apply(record));
    }

    public static Function<Record, Record> addColumn(String columnName, String staticValue) {
        return record -> record.put(columnName, staticValue);
    }

    public static Function<Record, Record> fillMissingValues(String columnName, String defaultValue) {
        return record -> record.putIfAbsent(columnName, defaultValue);
    }
}
