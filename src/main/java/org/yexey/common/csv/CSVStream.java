package org.yexey.common.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.yexey.common.csv.imp.CSVPrinter;
import org.yexey.common.csv.imp.CSVWriter;
import org.yexey.common.csv.imp.Record;
import org.yexey.common.csv.imp.ValidationError;
import org.yexey.common.csv.imp.exceptions.ColumnAlreadyExistsException;
import org.yexey.common.csv.imp.exceptions.ColumnNotFoundException;
import org.yexey.common.csv.imp.joins.CSVStreamFullJoin;
import org.yexey.common.csv.imp.joins.CSVStreamJoin;
import org.yexey.common.csv.imp.joins.CSVStreamLeftJoin;
import org.yexey.common.csv.imp.joins.CSVStreamRightJoin;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Writer;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVStream {

    private Stream<Record> stream;

    private CSVStream(Stream<Record> stream) {
        this.stream = stream;
    }

    public static CSVStream toCSVStream(Reader reader, CSVFormat csvFormat) throws IOException {
        CSVParser csvParser = new CSVParser(reader, csvFormat);
        Stream<CSVRecord> recordStream = csvParser.stream();
        return new CSVStream(recordStream.map(Record::new));
    }

    public void writeTo(Writer writer, CSVFormat csvFormat) throws IOException {
        CSVWriter.writeTo(stream, writer, csvFormat);
    }

    public CSVStream copy() {
        List<Record> recordsList = stream.collect(Collectors.toList());
        Stream<Record> newStream = recordsList.stream();
        this.stream = recordsList.stream();
        return new CSVStream(newStream);
    }

    public CSVStream deepCopy() {
        List<Record> recordsList = stream.collect(Collectors.toList());
        this.stream = recordsList.stream();
        return new CSVStream(recordsList.stream().map(Record::copy).collect(Collectors.toList()).stream());
    }

    public List<Record> toList() {
        return stream.collect(Collectors.toList());
    }

    public CSVStream peek(Consumer<Record> consumer) {
        return new CSVStream(stream.peek(consumer));
    }

    public Optional<Record> reduce(BinaryOperator<Record> accumulator) {
        Objects.requireNonNull(accumulator, "Accumulator must not be null");

        return stream.reduce(accumulator);
    }

    public Record reduce(Record identity, BinaryOperator<Record> accumulator) {
        Objects.requireNonNull(identity, "Identity must not be null");
        Objects.requireNonNull(accumulator, "Accumulator must not be null");

        return stream.reduce(identity, accumulator);
    }

    public <K> Map<K, List<Record>> groupBy(Function<Record, K> classifier) {
        return stream.collect(Collectors.groupingBy(classifier));
    }

    public <T> Stream<T> map(Function<Record, T> mapper) {
        return stream.map(mapper);
    }

    public CSVStream rename(String columnBefore, String columnAfter) {
        var tmp = stream.map(record -> {
            if (!record.containsColumn(columnBefore)) {
                throw new ColumnNotFoundException("Column " + columnBefore + " not found");
            }
            return record.rename(columnBefore, columnAfter);
        });
        return new CSVStream(tmp);
    }

    public CSVStream mapColumn(String column, Function<String, String> function) {
        var tmp = stream.map(record -> {
            if (!record.containsColumn(column)) {
                throw new ColumnNotFoundException("Column " + column + " not found");
            }
            return record.put(column, function.apply(record.get(column)));
        });
        return new CSVStream(tmp);
    }

    public CSVStream deleteColumns(String... columns) {
        var tmp = stream.map(record -> record.deleteColumns(columns));
        return new CSVStream(tmp);
    }

    public CSVStream retainColumn(String... columns) {
        var tmp = stream.map(record -> record.retainColumns(columns));
        return new CSVStream(tmp);
    }

    public CSVStream filter(Predicate<Record> predicate) {
        var tmp = stream.filter(predicate);
        return new CSVStream(tmp);
    }

    public CSVStream filter(String column, Predicate<String> predicate) {
        var tmp = stream.filter(record -> {
            if (!record.containsColumn(column)) {
                throw new ColumnNotFoundException("Column " + column + " not found");
            }
            return predicate.test(record.get(column));
        });
        return new CSVStream(tmp);
    }

    public CSVStream addColumn(String columnName, Function<Record, String> valueFunction) {
        var tmp = stream.map(record -> {
            if (record.containsColumn(columnName)) {
                throw new ColumnAlreadyExistsException("Column " + columnName + " is already present");
            }
            return record.put(columnName, valueFunction.apply(record));
        });
        return new CSVStream(tmp);
    }

    public CSVStream addColumn(String columnName, String staticValue) {
        var tmp = stream.map(record -> {
            if (record.containsColumn(columnName)) {
                throw new ColumnAlreadyExistsException("Column " + columnName + " is already present");
            }
            return record.put(columnName, staticValue);
        });
        return new CSVStream(tmp);
    }

    public CSVStream fillMissingValues(String columnName, String defaultValue) {
        var tmp = stream.map(record -> {
            if (!record.containsColumn(columnName)) {
                throw new ColumnNotFoundException("Column " + columnName + " not found");
            }
            return record.putIfAbsent(columnName, defaultValue);
        });
        return new CSVStream(tmp);
    }

    public CSVStream sort(String column, Comparator<String> comparator) {
        Comparator<Record> recordComparator = (record1, record2) -> comparator.compare(record1.get(column), (record2.get(column)));
        Stream<Record> tmp = stream.sorted(recordComparator);
        return new CSVStream(tmp);
    }

    public CSVStream sort(Comparator<Record> comparator) {
        var tmp = stream.sorted(comparator);
        return new CSVStream(tmp);
    }

    public CSVStream consumeAndContinue() {
        var tmp = stream.collect(Collectors.toList());
        return new CSVStream(tmp.stream());
    }

    public void consume() {
        stream.forEach((elm) -> {});
    }

    //-------------------------- Joining stuff
    public CSVStream join(CSVStream other, String keyColumnCSVA) {
        return this.join(other, keyColumnCSVA, keyColumnCSVA);
    }

    public CSVStream join(CSVStream other, String keyColumnCSVA, String keyColumnCSVB) {
        Stream<Record> resultStream = CSVStreamJoin.join(this.stream, other.stream, keyColumnCSVA, keyColumnCSVB);
        return new CSVStream(resultStream);
    }

    public CSVStream leftJoin(CSVStream other, String keyColumnCSVA) {
        return this.leftJoin(other, keyColumnCSVA, keyColumnCSVA);
    }

    public CSVStream leftJoin(CSVStream other, String keyColumnCSVA, String keyColumnCSVB) {
        Stream<Record> resultStream = CSVStreamLeftJoin.leftJoin(this.stream, other.stream, keyColumnCSVA, keyColumnCSVB);
        return new CSVStream(resultStream);
    }

    public CSVStream rightJoin(CSVStream other, String keyColumnCSVBoth) {
        return this.rightJoin(other, keyColumnCSVBoth, keyColumnCSVBoth);
    }

    public CSVStream rightJoin(CSVStream other, String keyColumnCSVA, String keyColumnCSVB) {
        Stream<Record> resultStream = CSVStreamRightJoin.rightJoin(this.stream, other.stream, keyColumnCSVA, keyColumnCSVB);
        return new CSVStream(resultStream);
    }

    public CSVStream fullJoin(CSVStream other, String keyColumnCSVA) {
        return this.fullJoin(other, keyColumnCSVA, keyColumnCSVA);
    }

    public CSVStream fullJoin(CSVStream other, String keyColumnCSVA, String keyColumnCSVB) {
        Stream<Record> resultStream = CSVStreamFullJoin.fullJoin(this.stream, other.stream, keyColumnCSVA, keyColumnCSVB);
        return new CSVStream(resultStream);
    }

    //-------------------------- Printing stuff
    public CSVStream printAsTable() {
        return printAsTable(System.out);
    }

    public CSVStream printAsTable(PrintStream ps) {
        List<Record> list = stream.collect(Collectors.toList());
        CSVPrinter.printAsTable(list, ps);
        return new CSVStream(list.stream());
    }

    public CSVStream printColumnsAsTable(String... columnNames) {
        return printColumnsAsTable(System.out, columnNames);
    }

    public CSVStream printColumnsAsTable(PrintStream ps, String... columnNames) {
        List<Record> list = stream.collect(Collectors.toList());
        CSVPrinter.printColumnsAsTable(list, ps, columnNames);
        return new CSVStream(list.stream());
    }

    public CSVStream printColumns(String... columnNames) {
        return printColumns(',', columnNames);
    }

    public CSVStream printColumns(PrintStream ps, String... columnNames) {
        return printColumns(ps, ',', columnNames);
    }

    public CSVStream printColumns(char delimiter, String... columnNames) {
        return printColumns(System.out, delimiter, columnNames);
    }

    public CSVStream printColumns(PrintStream ps, char delimiter, String... columnNames) {
        Objects.requireNonNull(ps, "PrintStream must not be null");
        Objects.requireNonNull(columnNames, "ColumnNames must not be null");

        Stream<Record> tmp = stream.peek(record -> CSVPrinter.printColumnsForSingleRecord(record, ps, delimiter, columnNames));
        return new CSVStream(tmp);
    }

    public CSVStream print() {
        return print(System.out, ',');
    }

    public CSVStream print(PrintStream ps) {
        return print(ps, ',');
    }

    public CSVStream print(char delimiter) {
        return print(System.out, delimiter);
    }

    public CSVStream print(PrintStream ps, char delimiter) {
        Stream<Record> tmp = stream.peek(record -> CSVPrinter.printSingleRecord(record, ps, delimiter));
        return new CSVStream(tmp);
    }

    //-------------------------- Validation Stuff
    public CSVStream validateAndThrowOnFailure(String column, Predicate<String> validator, Supplier<? extends RuntimeException> exceptionSupplier) {
        Objects.requireNonNull(column, "Column must not be null");
        Objects.requireNonNull(validator, "Validator must not be null");
        Objects.requireNonNull(exceptionSupplier, "Exception supplier must not be null");

        Stream<Record> validatedStream = stream.peek(record -> {
            String value = record.get(column);
            if (!validator.test(value)) {
                throw exceptionSupplier.get();
            }
        });
        return new CSVStream(validatedStream);
    }

    public CSVStream validateAndThrowOnFailure(Predicate<Record> validator, Supplier<? extends RuntimeException> exceptionSupplier) {
        Objects.requireNonNull(validator, "Validator must not be null");
        Objects.requireNonNull(exceptionSupplier, "Exception supplier must not be null");

        Stream<Record> validatedStream = stream.peek(record -> {
            if (!validator.test(record)) {
                throw exceptionSupplier.get();
            }
        });
        return new CSVStream(validatedStream);
    }

    public List<ValidationError> validateEager(String column, Predicate<String> validator, String errorMessage) {
        Objects.requireNonNull(column, "Column must not be null");
        Objects.requireNonNull(validator, "Validator must not be null");

        List<ValidationError> errors = new ArrayList<>();
        List<Record> records = stream.collect(Collectors.toList());
        for (Record record : records) {
            if (!record.containsColumn(column)) {
                errors.add(new ValidationError(record, "Column " + column + " not found"));
            } else {
                String value = record.get(column);
                if (!validator.test(value)) {
                    String message = errorMessage != null ? errorMessage : "Validation failed for column: " + column;
                    errors.add(new ValidationError(record, message));
                }
            }
        }
        // Re-create the stream with collected records
        this.stream = records.stream();
        return errors;
    }
}
