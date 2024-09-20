package org.yexey.common.util.csv.imp;

import org.apache.commons.csv.CSVRecord;
import org.yexey.common.util.string.StringUtils;
import org.yexey.common.util.csv.Record;

import java.io.PrintStream;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RecordCSVUtil {

    public static List<Record> toListLinkedHashMap(Collection<CSVRecord> csvRecordsStream, List<String> headers) {
        List<Record> csvRecords = new ArrayList<>();
        for(var record : csvRecordsStream) {
            LinkedHashMap<String, String> res = new LinkedHashMap<>(headers.size());
            for(var header : headers) {
                res.put(header, record.get(header));
            }
            csvRecords.add(new Record(res));
        }
        return csvRecords;
    }

    public static void printAsTable(List<Record> csvRecords, PrintStream ps) {
        if(csvRecords == null || csvRecords.isEmpty()) return;
        printColumnsAsTable(csvRecords, ps, csvRecords.getFirst().getColumnNames().toArray(new String[0]));
    }

    public static void printColumnsAsTable(List<Record> csvRecords, PrintStream ps, String... columnNames) {
        // Step 1: Compute the maximum width for each column
        Map<String, Integer> columnWidths = new LinkedHashMap<>();
        // Initialize with the length of the column names
        for (String columnName : columnNames) {
            columnWidths.put(columnName, columnName.length());
        }
        // Update with the maximum length of the data in each column
        for (Record record : csvRecords) {
            for (String columnName : columnNames) {
                String value = record.get(columnName);
                if (value == null) value = ""; // Handle nulls
                int length = value.length();
                if (length > columnWidths.get(columnName)) {
                    columnWidths.put(columnName, length);
                }
            }
        }
        // Step 2: Print the header line
        StringBuilder header = new StringBuilder();
        for (String columnName : columnNames) {
            int width = columnWidths.get(columnName);
            header.append(String.format("%-" + width + "s  ", columnName));
        }
        ps.println(header);
        // Step 3: Print a separator line
        StringBuilder separator = new StringBuilder();
        for (String columnName : columnNames) {
            int width = columnWidths.get(columnName);
            for (int i = 0; i < width; i++) {
                separator.append("-");
            }
            separator.append("  ");
        }
        ps.println(separator);
        // Step 4: Print each record
        for (Record record : csvRecords) {
            StringBuilder line = new StringBuilder();
            for (String columnName : columnNames) {
                int width = columnWidths.get(columnName);
                String value = record.get(columnName);
                if (value == null) value = "";
                line.append(String.format("%-" + width + "s  ", value));
            }
            ps.println(line);
        }
    }

    public static void printColumnsForSingleRecord(Record csvRecord, PrintStream ps, String... columnNames) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columnNames.length; i++) {
            var columnName = columnNames[i];
            sb.append(csvRecord.get(columnName));
            if (i < columnNames.length - 1) {
                sb.append(',');
            }
        }
        ps.println(sb);
    }

    public static void printSingleRecord(Record csvRecord, PrintStream ps) {
        StringBuilder sb = new StringBuilder();
        for (Iterator<String> iterator = csvRecord.getValues().iterator(); iterator.hasNext(); ) {
            String value = iterator.next();
            sb.append(value);
            if (iterator.hasNext()) {
                sb.append(',');
            }
        }
        ps.println(sb);
    }
   
    public static void printColumns(List<Record> csvRecords, PrintStream ps, String... columnNames) {
        StringBuilder sb = new StringBuilder(StringUtils.join(columnNames, ","));
        sb.append(System.lineSeparator());
        for(Record elm : csvRecords){
            for (int i = 0; i < columnNames.length; i++) {
                var columnName = columnNames[i];
                sb.append(elm.get(columnName));
                if (i < columnNames.length - 1) {
                    sb.append(',');
                }
            }
            sb.append(System.lineSeparator());
        }
        ps.println(sb);
    }

   
    public static void print(List<Record> csvRecords, PrintStream ps) {
        if(csvRecords == null || csvRecords.isEmpty()) return;
        printColumns(csvRecords, ps, csvRecords.getFirst().getColumnNames().toArray(new String[0]));
    }

   
    public static void rename(List<Record> csvRecords, String columnBefore, String columnAfter) {
        for(Record elm : csvRecords){
            elm.put(columnAfter, elm.get(columnAfter));
            elm.deleteColumn(columnBefore);
        }
    }

   
    public static void mapColumn(List<Record> csvRecords, String column, Function<String, String> function) {
        for(Record elm : csvRecords){
            elm.put(column, function.apply(elm.get(column)));
        }
    }

   
    public static void deleteColumns(List<Record> csvRecords, String... columns) {
        for(Record elm : csvRecords) {
            for (String column : columns) {
                elm.deleteColumn(column);
            }
        }
    }

   
    public static void retainColumn(List<Record> csvRecords, String... columns) {
        for(Record elm : csvRecords) {
            for (String column : columns) {
                elm.deleteColumn(column);
            }
        }
    }

    // Method to perform an inner join
   
    public static List<Record> join(List<Record> csvRecords, List<Record> other, String keyColumnCSVA, String keyColumnCSVB) {
        // Map to store records from 'other' based on the keyColumn
        Map<String, Record> otherKeyValueRecordsMap = new HashMap<>();
        for (Record record : other) {
            String key = record.get(keyColumnCSVB);
            if (key != null) {
                otherKeyValueRecordsMap.put(key, record);
            }
        }

        // List to store the joined records
        List<Record> newRecords = new ArrayList<>();

        for (Record record : csvRecords) {
            String keyValue = record.get(keyColumnCSVA);
            if (keyValue != null && otherKeyValueRecordsMap.containsKey(keyValue)) {
                Map<String, String> otherRecord = otherKeyValueRecordsMap.get(keyValue).getData();
                // Merge records
                Record mergedRecord = record.clone();
                for (Map.Entry<String, String> entry : otherRecord.entrySet()) {
                    String columnName = entry.getKey();
                    if (!columnName.equals(keyColumnCSVB)) {
                        // Optionally, handle column name conflicts
                        mergedRecord.put(columnName, entry.getValue());
                    }
                }
                newRecords.add(mergedRecord);
            }
        }

        // Update the records with the joined data
        return newRecords;
    }

    // Method to perform a left outer join
   
    public static List<Record> leftJoin(List<Record> csvRecords, List<Record> other, String keyColumnCSVA, String keyColumnCSVB) {
        // Map to store records from 'other' based on keyColumnCSVB
        Map<String, Record> otherRecordsMap = new HashMap<>();
        for (Record record : other) {
            String key = record.get(keyColumnCSVB);
            if (key != null) {
                otherRecordsMap.put(key, record);
            }
        }

        // List to store the joined records
        List<Record> newRecords = new ArrayList<>();

        for (Record record : csvRecords) {
            String key = record.get(keyColumnCSVA);
            LinkedHashMap<String, String> mergedRecord = new LinkedHashMap<>(record.getData());
            if (key != null && otherRecordsMap.containsKey(key)) {
                Map<String, String> otherRecord = otherRecordsMap.get(key).getData();
                // Merge records
                for (Map.Entry<String, String> entry : otherRecord.entrySet()) {
                    String columnName = entry.getKey();
                    if (!columnName.equals(keyColumnCSVB)) {
                        // Optionally, handle column name conflicts
                        mergedRecord.put(columnName, entry.getValue());
                    }
                }
            }
            // Else, keep the original record (columns from 'other' will be missing)
            newRecords.add(new Record(mergedRecord));
        }

        // Update the records with the joined data
        return newRecords;
    }



    public static void filter(List<Record> csvRecords, Predicate<Record> predicate) {
        csvRecords.removeIf((elm) -> !predicate.test(elm));
    }


    public static void sort(List<Record> csvRecords, Comparator<Record> comparator) {
        csvRecords.sort(comparator);
    }

    public static void addColumn(List<Record> csvRecords, String columnName, Function<Record, String> valueFunction) {
        for (Record record : csvRecords) {
            record.put(columnName, valueFunction.apply(record));
        }
    }

   
    public static void addColumn(List<Record> csvRecords, String columnName, String staticValue) {
        for (Record record : csvRecords) {
            record.put(columnName, staticValue);
        }
    }

   
    public static void fillMissingValues(List<Record> csvRecords, String columnName, String defaultValue) {
        for (Record record : csvRecords) {
            record.putIfAbsent(columnName, defaultValue);
            if (record.get(columnName) == null || record.get(columnName).isEmpty()) {
                record.put(columnName, defaultValue);
            }
        }
    }
}
