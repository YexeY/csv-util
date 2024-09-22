package org.yexey.common.csv.imp;

import org.apache.commons.csv.CSVRecord;

import java.util.*;

public class Record {

    private final LinkedHashMap<String, String> data;

    // Constructor initializing with an existing map
    public Record(Map<String, String> initialData) {
        this.data = new LinkedHashMap<>(initialData);
    }

    public Record copy() {
        return new Record(new HashMap<>(this.data));
    }

    public Record(CSVRecord value) {
        this.data = new LinkedHashMap<>(value.size());
        // Iterate through the CSVRecord and populate the LinkedHashMap
        for (String header : value.getParser().getHeaderMap().keySet()) {
            this.data.put(header, value.get(header));
        }
    }

    // Constructor initializing an empty record
    public Record() {
        this.data = new LinkedHashMap<>();
    }

    // Get value by column name
    public String get(String columnName) {
        return data.get(columnName);
    }

    // Set value by column name
    public Record set(String columnName, String value) {
        data.put(columnName, value);
        return this;
    }
    public Record put(String columnName, String value) {
        set(columnName, value);
        return this;
    }

    public Record putIfAbsent(String columnName, String defaultValue) {
        this.data.compute(columnName, (k, v) -> v != null && !v.trim().isEmpty() ? v : defaultValue);
        return this;
    }

    // Check if the column exists
    public boolean containsColumn(String columnName) {
        return data.containsKey(columnName);
    }

    // Remove a column
    public Record deleteColumn(String columnName) {
        data.remove(columnName);
        return this;
    }

    public Record deleteColumns(String... columnNames) {
        for(var columnName : columnNames) {
            data.remove(columnName);
        }
        return this;
    }

    public Record retainColumns(String... columnNames) {
        List<String> list = Arrays.stream(columnNames).toList();
        for(String column : new ArrayList<>(getColumnNames())) {
            if(!list.contains(column)) {
                this.deleteColumn(column);
            }
        }
        return this;
    }

    public Record rename(String columnNameOld, String columnNameNew) {
        String value = data.remove(columnNameOld);
        data.put(columnNameNew, value);
        return this;
    }

    // Get all column names
    public Set<String> getColumnNames() {
        return data.keySet();
    }

    // Get all values
    public Collection<String> getValues() {
        return data.values();
    }

    // Get the underlying data map (if needed)
    public Map<String, String> getData() {
        return data;
    }

    // Clone the record
    public Record clone() {
        return new Record(new LinkedHashMap<>(data));
    }

    // Override toString for easy printing
    @Override
    public String toString() {
        return data.toString();
    }

    // Equals and hashCode methods (optional, for comparison and collections)
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Record)) return false;
        Record other = (Record) obj;
        return data.equals(other.data);
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }
}