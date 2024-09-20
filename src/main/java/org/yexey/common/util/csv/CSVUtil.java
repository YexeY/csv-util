package org.yexey.common.util.csv;

import org.apache.commons.csv.CSVRecord;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public interface CSVUtil {
    List<LinkedHashMap<String, String>> toListLinkedHashMap(Collection<CSVRecord> csvRecordsStream, List<String> headers);

    void printAsTable(List<LinkedHashMap<String, String>> csvRecords, PrintStream ps);

    void printColumnsAsTable(List<LinkedHashMap<String, String>> csvRecords, PrintStream ps, String... columnNames);

    void printColumns(List<LinkedHashMap<String, String>> csvRecords, PrintStream ps, String... columnNames);

    void print(List<LinkedHashMap<String, String>> csvRecords, PrintStream ps);

    void rename(List<LinkedHashMap<String, String>> csvRecords, String columnBefore, String columnAfter);

    void mapColumn(List<LinkedHashMap<String, String>> csvRecords, String column, Function<String, String> function);

    void deleteColumns(List<LinkedHashMap<String, String>> csvRecords, String... columns);

    void retainColumn(List<LinkedHashMap<String, String>> csvRecords, String... columns);

    // Method to perform an inner join
    List<LinkedHashMap<String, String>> join(List<LinkedHashMap<String, String>> csvRecords, List<LinkedHashMap<String, String>> other, String keyColumnCSVA, String keyColumnCSVB);

    // Method to perform a left outer join
    List<LinkedHashMap<String, String>> leftJoin(List<LinkedHashMap<String, String>> csvRecords, List<LinkedHashMap<String, String>> other, String keyColumnCSVA, String keyColumnCSVB);

    void filter(List<LinkedHashMap<String, String>> csvRecords, Predicate<LinkedHashMap<String, String>> predicate);

    void sort(List<LinkedHashMap<String, String>> csvRecords, Comparator<LinkedHashMap<String, String>> comparator);

    void addColumn(List<LinkedHashMap<String, String>> csvRecords, String columnName, Function<LinkedHashMap<String, String>, String> valueFunction);

    void addColumn(List<LinkedHashMap<String, String>> csvRecords, String columnName, String staticValue);

    void fillMissingValues(List<LinkedHashMap<String, String>> csvRecords, String columnName, String defaultValue);
}
