package org.yexey.common.util.csv.imp;

import org.apache.commons.csv.CSVRecord;
import org.yexey.common.util.csv.CSVUtil;
import org.yexey.common.util.string.StringUtils;

import java.io.PrintStream;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

public class SimpleCSVUtil implements CSVUtil {

    @Override
    public List<LinkedHashMap<String, String>> toListLinkedHashMap(Collection<CSVRecord> csvRecordsStream, List<String> headers) {
        List<LinkedHashMap<String, String>> csvRecords = new ArrayList<>();
        for(var record : csvRecordsStream) {
            LinkedHashMap<String, String> res = new LinkedHashMap<>(headers.size());
            for(var header : headers) {
                res.put(header, record.get(header));
            }
            csvRecords.add(res);
        }
        return csvRecords;
    }

    @Override
    public void printAsTable(List<LinkedHashMap<String, String>> csvRecords, PrintStream ps) {
        if(csvRecords == null || csvRecords.isEmpty()) return;
        printColumnsAsTable(csvRecords, ps, csvRecords.getFirst().keySet().toArray(new String[0]));
    }

    @Override
    public void printColumnsAsTable(List<LinkedHashMap<String, String>> csvRecords, PrintStream ps, String... columnNames) {
        // Step 1: Compute the maximum width for each column
        Map<String, Integer> columnWidths = new LinkedHashMap<>();
        // Initialize with the length of the column names
        for (String columnName : columnNames) {
            columnWidths.put(columnName, columnName.length());
        }
        // Update with the maximum length of the data in each column
        for (LinkedHashMap<String, String> record : csvRecords) {
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
        for (LinkedHashMap<String, String> record : csvRecords) {
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

    @Override
    public void printColumns(List<LinkedHashMap<String, String>> csvRecords, PrintStream ps, String... columnNames) {
        StringBuilder sb = new StringBuilder(StringUtils.join(columnNames, ","));
        for(LinkedHashMap<String, String> elm : csvRecords){
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

    @Override
    public void print(List<LinkedHashMap<String, String>> csvRecords, PrintStream ps) {
        if(csvRecords == null || csvRecords.isEmpty()) return;
        printColumns(csvRecords, ps, csvRecords.getFirst().keySet().toArray(new String[0]));
    }

    @Override
    public void rename(List<LinkedHashMap<String, String>> csvRecords, String columnBefore, String columnAfter) {
        for(LinkedHashMap<String, String> elm : csvRecords){
            elm.put(columnAfter, elm.get(columnAfter));
            elm.remove(columnBefore);
        }
    }

    @Override
    public void mapColumn(List<LinkedHashMap<String, String>> csvRecords, String column, Function<String, String> function) {
        for(LinkedHashMap<String, String> elm : csvRecords){
            elm.put(column, function.apply(elm.get(column)));
        }
    }

    @Override
    public void deleteColumns(List<LinkedHashMap<String, String>> csvRecords, String... columns) {
        for(LinkedHashMap<String, String> elm : csvRecords) {
            for (String column : columns) {
                elm.remove(column);
            }
        }
    }

    @Override
    public void retainColumn(List<LinkedHashMap<String, String>> csvRecords, String... columns) {
        for(LinkedHashMap<String, String> elm : csvRecords) {
            for (String column : columns) {
                elm.remove(column);
            }
        }
    }

    // Method to perform an inner join
    @Override
    public List<LinkedHashMap<String, String>> join(List<LinkedHashMap<String, String>> csvRecords, List<LinkedHashMap<String, String>> other, String keyColumnCSVA, String keyColumnCSVB) {
        // Map to store records from 'other' based on the keyColumn
        Map<String, LinkedHashMap<String, String>> otherKeyValueRecordsMap = new HashMap<>();
        for (LinkedHashMap<String, String> record : other) {
            String key = record.get(keyColumnCSVB);
            if (key != null) {
                otherKeyValueRecordsMap.put(key, record);
            }
        }

        // List to store the joined records
        List<LinkedHashMap<String, String>> newRecords = new ArrayList<>();

        for (LinkedHashMap<String, String> record : csvRecords) {
            String keyValue = record.get(keyColumnCSVA);
            if (keyValue != null && otherKeyValueRecordsMap.containsKey(keyValue)) {
                LinkedHashMap<String, String> otherRecord = otherKeyValueRecordsMap.get(keyValue);
                // Merge records
                LinkedHashMap<String, String> mergedRecord = new LinkedHashMap<>(record);
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
    @Override
    public List<LinkedHashMap<String, String>> leftJoin(List<LinkedHashMap<String, String>> csvRecords, List<LinkedHashMap<String, String>> other, String keyColumnCSVA, String keyColumnCSVB) {
        // Map to store records from 'other' based on keyColumnCSVB
        Map<String, LinkedHashMap<String, String>> otherRecordsMap = new HashMap<>();
        for (LinkedHashMap<String, String> record : other) {
            String key = record.get(keyColumnCSVB);
            if (key != null) {
                otherRecordsMap.put(key, record);
            }
        }

        // List to store the joined records
        List<LinkedHashMap<String, String>> newRecords = new ArrayList<>();

        for (LinkedHashMap<String, String> record : csvRecords) {
            String key = record.get(keyColumnCSVA);
            LinkedHashMap<String, String> mergedRecord = new LinkedHashMap<>(record);
            if (key != null && otherRecordsMap.containsKey(key)) {
                LinkedHashMap<String, String> otherRecord = otherRecordsMap.get(key);
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
            newRecords.add(mergedRecord);
        }

        // Update the records with the joined data
        return newRecords;
    }

    @Override
    public void filter(List<LinkedHashMap<String, String>> csvRecords, Predicate<LinkedHashMap<String, String>> predicate) {
        csvRecords.removeIf((elm) -> !predicate.test(elm));
    }

    @Override
    public void sort(List<LinkedHashMap<String, String>> csvRecords, Comparator<LinkedHashMap<String, String>> comparator) {
        csvRecords.sort(comparator);
    }

    @Override
    public void addColumn(List<LinkedHashMap<String, String>> csvRecords, String columnName, Function<LinkedHashMap<String, String>, String> valueFunction) {
        for (LinkedHashMap<String, String> record : csvRecords) {
            record.put(columnName, valueFunction.apply(record));
        }
    }

    @Override
    public void addColumn (List<LinkedHashMap<String, String>> csvRecords, String columnName, String staticValue) {
        for (LinkedHashMap<String, String> record : csvRecords) {
            record.put(columnName, staticValue);
        }
    }

    @Override
    public void fillMissingValues(List<LinkedHashMap<String, String>> csvRecords, String columnName, String defaultValue) {
        for (LinkedHashMap<String, String> record : csvRecords) {
            record.putIfAbsent(columnName, defaultValue);
            if (record.get(columnName) == null || record.get(columnName).isEmpty()) {
                record.put(columnName, defaultValue);
            }
        }
    }
}
