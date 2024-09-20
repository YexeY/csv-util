package org.yexey.common.util.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.yexey.common.util.csv.imp.RecordCSVUtil;
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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVStream {
    private final PrintStream ps;

    private Stream<Record> stream;
    private final List<ValidationError> validationErrors;

    protected CSVStream(Stream<Record> stream) {
        this.stream = stream;
        ps = System.out;
        validationErrors = Collections.synchronizedList(new ArrayList<>());
    }

    public CSVStream copy() {
        List<Record> recordsList = stream.toList();
        Stream<Record> newStream = recordsList.stream();
        this.stream = recordsList.stream();
        return new CSVStream(newStream);
    }

    public CSVStream deepCopy() {
        List<Record> recordsList = stream.toList();
        this.stream = recordsList.stream();
        return new CSVStream(recordsList.stream().map(Record::copy));
    }

    public static CSVStream toCSVStream(Reader reader, CSVFormat csvFormat) throws IOException {
        CSVParser csvParser = new CSVParser(reader, csvFormat);
        Stream<CSVRecord> recordIterator = csvParser.stream();
        return new CSVStream(recordIterator.map(Record::new));
    }


    public List<ValidationError> getValidationErrors() {
        return validationErrors;
    }

    public CSVStream validate(String column, Predicate<String> validator, String errorMessage) {
        Stream<Record> validatedStream = stream.peek(record -> {
            String value = record.get(column);
            if (!validator.test(value)) {
                String message = errorMessage != null ? errorMessage : "Validation failed for column: " + column;
                validationErrors.add(new ValidationError(record, message));
            }
        });
        return new CSVStream(validatedStream);
    }

    public CSVStream validate(Predicate<Record> validator, String errorMessage) {
        Stream<Record> validatedStream = stream.peek(record -> {
            if (!validator.test(record)) {
                String message = errorMessage != null ? errorMessage : "Validation failed for record";
                validationErrors.add(new ValidationError(record, message));
            }
        });
        return new CSVStream(validatedStream);
    }

    public Record reduce(Record identity, BinaryOperator<Record> accumulator) {
        return stream.reduce(identity, accumulator);
    }

    public <K> Map<K, List<Record>> groupBy(Function<Record, K> classifier) {
        return stream.collect(Collectors.groupingBy(classifier));
    }

    public <T> Stream<T> mapInto(Function<Record, T> mapper) {
        return stream.map(mapper);
    }

    public static CSVStream ofCSVRecords(Collection<CSVRecord> records) {
        return new CSVStream(records.stream().map(elm -> new Record(elm.toMap())));
    }

    public static CSVStream of(Collection<Record> records) {
        return new CSVStream(records.stream());
    }

    public CSVStream rename(String columnBefore, String columnAfter) {
        var tmp = stream.map(CSVUtilFunctionBuilder.rename(columnBefore, columnAfter));
        return new CSVStream(tmp);
    }

    public CSVStream mapColumn(String column, Function<String, String> function) {
        var tmp = stream.map(CSVUtilFunctionBuilder.mapColumn(column, function));
        return new CSVStream(tmp);
    }

    public CSVStream deleteColumns(String... columns) {
        var tmp = stream.map(CSVUtilFunctionBuilder.deleteColumns(columns));
        return new CSVStream(tmp);
    }

    public CSVStream retainColumn(String... columns) {
        var tmp = stream.map(CSVUtilFunctionBuilder.retainColumn(columns));
        return new CSVStream(tmp);
    }

    public CSVStream filter(String column, Predicate<String> predicate) {
        var tmp = stream.filter(CSVUtilFunctionBuilder.filter(column, predicate));
        return new CSVStream(tmp);
    }

    public CSVStream addColumn(String columnName, Function<Record, String> valueFunction) {
        var tmp = stream.map(CSVUtilFunctionBuilder.addColumn(columnName, valueFunction));
        return new CSVStream(tmp);
    }

    public CSVStream addColumn(String columnName, String staticValue) {
        var tmp = stream.map(CSVUtilFunctionBuilder.addColumn(columnName, staticValue));
        return new CSVStream(tmp);
    }

    public CSVStream fillMissingValues(String columnName, String defaultValue) {
        var tmp = stream.map(CSVUtilFunctionBuilder.fillMissingValues(columnName, defaultValue));
        return new CSVStream(tmp);
    }

    public CSVStream printAsTable() {
        List<Record> list = stream.collect(Collectors.toList());
        RecordCSVUtil.printAsTable(list, ps);
        return new CSVStream(list.stream());
    }

    public CSVStream printColumnsAsTable(String... columnNames) {
        List<Record> list = stream.collect(Collectors.toList());
        RecordCSVUtil.printColumnsAsTable(list, ps, columnNames);
        return new CSVStream(list.stream());
    }

    public CSVStream printColumns(String... columnNames) {
        Stream<Record> tmp = stream.peek(record -> RecordCSVUtil.printColumnsForSingleRecord(record, ps, columnNames));
        return new CSVStream(tmp);
    }

    public CSVStream print() {
        Stream<Record> tmp = stream.peek(record -> RecordCSVUtil.printSingleRecord(record, ps));
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

    public CSVStream rightJoin(CSVStream other, String keyColumnCSVA) {
        return this.rightJoin(other, keyColumnCSVA, keyColumnCSVA);
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

    public CSVStream consumeAndContinue() {
        var tmp = stream.toList();
        return new CSVStream(tmp.stream());
    }

    public void consume() {
        stream.forEach(_ -> {});
    }
}
