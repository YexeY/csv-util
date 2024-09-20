package org.yexey.common.util.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.yexey.common.util.csv.imp.*;
import org.yexey.common.util.csv.imp.Record;
import org.yexey.common.util.csv.imp.joins.CSVStreamFullJoin;
import org.yexey.common.util.csv.imp.joins.CSVStreamJoin;
import org.yexey.common.util.csv.imp.joins.CSVStreamLeftJoin;
import org.yexey.common.util.csv.imp.joins.CSVStreamRightJoin;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Writer;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVStream {
    private final PrintStream ps;

    private Stream<org.yexey.common.util.csv.imp.Record> stream;
    private final List<ValidationError> validationErrors;

    protected CSVStream(Stream<org.yexey.common.util.csv.imp.Record> stream, List<ValidationError> validationErrors) {
        this.stream = stream;
        ps = System.out;
        this.validationErrors = validationErrors;
    }

    protected CSVStream(Stream<org.yexey.common.util.csv.imp.Record> stream) {
        this(stream, Collections.synchronizedList(new ArrayList<>()));
    }

    public static CSVStream toCSVStream(Reader reader, CSVFormat csvFormat) throws IOException {
        CSVParser csvParser = new CSVParser(reader, csvFormat);
        Stream<CSVRecord> recordStream = csvParser.stream();
        return new CSVStream(recordStream.map(org.yexey.common.util.csv.imp.Record::new));
    }

    public static CSVStream ofCSVRecords(Collection<CSVRecord> records) {
        return new CSVStream(records.stream().map(elm -> new org.yexey.common.util.csv.imp.Record(elm.toMap())));
    }

    public static CSVStream of(Collection<org.yexey.common.util.csv.imp.Record> records) {
        return new CSVStream(records.stream());
    }

    public void writeTo(Writer writer, CSVFormat csvFormat) throws IOException {
        CSVWriter.writeTo(stream, writer, csvFormat);
    }

    public CSVStream copy() {
        List<org.yexey.common.util.csv.imp.Record> recordsList = stream.toList();
        Stream<org.yexey.common.util.csv.imp.Record> newStream = recordsList.stream();
        this.stream = recordsList.stream();
        return new CSVStream(newStream, validationErrors);
    }

    public CSVStream deepCopy() {
        List<org.yexey.common.util.csv.imp.Record> recordsList = stream.toList();
        this.stream = recordsList.stream();
        return new CSVStream(recordsList.stream().map(org.yexey.common.util.csv.imp.Record::copy));
    }

    public List<org.yexey.common.util.csv.imp.Record> toList() {
        return stream.toList();
    }

    public CSVStream peek(Consumer<org.yexey.common.util.csv.imp.Record> consumer) {
        return new CSVStream(stream.peek(consumer), validationErrors);
    }

    public List<ValidationError> getValidationErrors() {
        return validationErrors;
    }

    public CSVStream validate(String column, Predicate<String> validator, String errorMessage) {
        if(validator == null) {
            throw new NullPointerException("Validator must not be null");
        }
        Stream<org.yexey.common.util.csv.imp.Record> validatedStream = stream.peek(record -> {
            String value = record.get(column);
            if (!validator.test(value)) {
                String message = errorMessage != null ? errorMessage : "Validation failed for column: " + column;
                validationErrors.add(new ValidationError(record, message));
            }
        });
        return new CSVStream(validatedStream, validationErrors);
    }

    public CSVStream validate(Predicate<org.yexey.common.util.csv.imp.Record> validator, String errorMessage) {
        if(validator == null) {
            throw new NullPointerException("Validator must not be null");
        }
        Stream<org.yexey.common.util.csv.imp.Record> validatedStream = stream.peek(record -> {
            if (!validator.test(record)) {
                String message = errorMessage != null ? errorMessage : "Validation failed for record";
                validationErrors.add(new ValidationError(record, message));
            }
        });
        return new CSVStream(validatedStream, validationErrors);
    }

    public org.yexey.common.util.csv.imp.Record reduce(org.yexey.common.util.csv.imp.Record identity, BinaryOperator<org.yexey.common.util.csv.imp.Record> accumulator) {
        return stream.reduce(identity, accumulator);
    }

    public <K> Map<K, List<org.yexey.common.util.csv.imp.Record>> groupBy(Function<org.yexey.common.util.csv.imp.Record, K> classifier) {
        return stream.collect(Collectors.groupingBy(classifier));
    }

    public <T> Stream<T> map(Function<org.yexey.common.util.csv.imp.Record, T> mapper) {
        return stream.map(mapper);
    }

    public CSVStream rename(String columnBefore, String columnAfter) {
        var tmp = stream.map(CSVUtilFunctionBuilder.rename(columnBefore, columnAfter));
        return new CSVStream(tmp, validationErrors);
    }

    public CSVStream mapColumn(String column, Function<String, String> function) {
        var tmp = stream.map(CSVUtilFunctionBuilder.mapColumn(column, function));
        return new CSVStream(tmp, validationErrors);
    }

    public CSVStream deleteColumns(String... columns) {
        var tmp = stream.map(CSVUtilFunctionBuilder.deleteColumns(columns));
        return new CSVStream(tmp, validationErrors);
    }

    public CSVStream retainColumn(String... columns) {
        var tmp = stream.map(CSVUtilFunctionBuilder.retainColumn(columns));
        return new CSVStream(tmp, validationErrors);
    }

    public CSVStream filter(Predicate<org.yexey.common.util.csv.imp.Record> predicate) {
      var tmp = stream.filter(predicate);
      return new CSVStream(tmp, validationErrors);
    }

    public CSVStream filter(String column, Predicate<String> predicate) {
        var tmp = stream.filter(CSVUtilFunctionBuilder.filter(column, predicate));
        return new CSVStream(tmp, validationErrors);
    }

    public CSVStream addColumn(String columnName, Function<org.yexey.common.util.csv.imp.Record, String> valueFunction) {
        var tmp = stream.map(CSVUtilFunctionBuilder.addColumn(columnName, valueFunction));
        return new CSVStream(tmp, validationErrors);
    }

    public CSVStream addColumn(String columnName, String staticValue) {
        var tmp = stream.map(CSVUtilFunctionBuilder.addColumn(columnName, staticValue));
        return new CSVStream(tmp, validationErrors);
    }

    public CSVStream fillMissingValues(String columnName, String defaultValue) {
        var tmp = stream.map(CSVUtilFunctionBuilder.fillMissingValues(columnName, defaultValue));
        return new CSVStream(tmp, validationErrors);
    }

    public CSVStream printAsTable() {
        List<org.yexey.common.util.csv.imp.Record> list = stream.collect(Collectors.toList());
        RecordCSVUtil.printAsTable(list, ps);
        return new CSVStream(list.stream(), validationErrors);
    }

    public CSVStream printColumnsAsTable(String... columnNames) {
        List<org.yexey.common.util.csv.imp.Record> list = stream.collect(Collectors.toList());
        RecordCSVUtil.printColumnsAsTable(list, ps, columnNames);
        return new CSVStream(list.stream(), validationErrors);
    }

    public CSVStream printColumns(String... columnNames) {
        Stream<org.yexey.common.util.csv.imp.Record> tmp = stream.peek(record -> RecordCSVUtil.printColumnsForSingleRecord(record, ps, columnNames));
        return new CSVStream(tmp, validationErrors);
    }

    public CSVStream print() {
        Stream<org.yexey.common.util.csv.imp.Record> tmp = stream.peek(record -> RecordCSVUtil.printSingleRecord(record, ps));
        return new CSVStream(tmp, validationErrors);
    }

    public CSVStream sort(String column, Comparator<String> comparator) {
        Comparator<org.yexey.common.util.csv.imp.Record> recordComparator = (record1, record2) -> comparator.compare(record1.get(column), (record2.get(column)));
        Stream<org.yexey.common.util.csv.imp.Record> tmp = stream.sorted(recordComparator);
        return new CSVStream(tmp, validationErrors);
    }

    public CSVStream sort(Comparator<org.yexey.common.util.csv.imp.Record> comparator) {
        var tmp = stream.sorted(comparator);
        return new CSVStream(tmp, validationErrors);
    }

    public CSVStream join(CSVStream other, String keyColumnCSVA) {
        return this.join(other, keyColumnCSVA, keyColumnCSVA);
    }

    public CSVStream join(CSVStream other, String keyColumnCSVA, String keyColumnCSVB) {
        Stream<org.yexey.common.util.csv.imp.Record> resultStream = CSVStreamJoin.join(this.stream, other.stream, keyColumnCSVA, keyColumnCSVB);
        return new CSVStream(resultStream, validationErrors);
    }

    public CSVStream leftJoin(CSVStream other, String keyColumnCSVA) {
        return this.leftJoin(other, keyColumnCSVA, keyColumnCSVA);
    }

    public CSVStream leftJoin(CSVStream other, String keyColumnCSVA, String keyColumnCSVB) {
        Stream<org.yexey.common.util.csv.imp.Record> resultStream = CSVStreamLeftJoin.leftJoin(this.stream, other.stream, keyColumnCSVA, keyColumnCSVB);
        return new CSVStream(resultStream, validationErrors);
    }

    public CSVStream rightJoin(CSVStream other, String keyColumnCSVBoth) {
        return this.rightJoin(other, keyColumnCSVBoth, keyColumnCSVBoth);
    }

    public CSVStream rightJoin(CSVStream other, String keyColumnCSVA, String keyColumnCSVB) {
        Stream<org.yexey.common.util.csv.imp.Record> resultStream = CSVStreamRightJoin.rightJoin(this.stream, other.stream, keyColumnCSVA, keyColumnCSVB);
        return new CSVStream(resultStream, validationErrors);
    }

    public CSVStream fullJoin(CSVStream other, String keyColumnCSVA) {
        return this.fullJoin(other, keyColumnCSVA, keyColumnCSVA);
    }

    public CSVStream fullJoin(CSVStream other, String keyColumnCSVA, String keyColumnCSVB) {
        Stream<Record> resultStream = CSVStreamFullJoin.fullJoin(this.stream, other.stream, keyColumnCSVA, keyColumnCSVB);
        return new CSVStream(resultStream, validationErrors);
    }

    public CSVStream consumeAndContinue() {
        var tmp = stream.toList();
        return new CSVStream(tmp.stream());
    }

    public void consume() {
        stream.forEach(_ -> {});
    }
}
