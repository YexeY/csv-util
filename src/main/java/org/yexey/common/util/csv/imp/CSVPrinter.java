package org.yexey.common.util.csv.imp;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CSVPrinter {


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
}
