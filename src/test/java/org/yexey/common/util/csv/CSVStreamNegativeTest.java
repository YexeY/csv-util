package org.yexey.common.util.csv;

import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Test;
import org.yexey.common.util.csv.imp.Record;
import org.yexey.common.util.csv.imp.ValidationError;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class CSVStreamNegativeTest {

    @Test
    void testToCSVStreamWithOnlyHeaders() throws IOException {
        String csvData = "Name,Age,Country";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        List<org.yexey.common.util.csv.imp.Record> records = csvStream.toList();
        assertEquals(0, records.size());
    }

    @Test
    void testRenameWithNonexistentColumn() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Attempt to rename a column that doesn't exist
        assertThrows(NullPointerException.class, () -> {
            csvStream.rename("Height", "Stature")
                    .mapColumn("Height", val -> val.toUpperCase())
                    .toList();
        });
    }

    @Test
    void testMapColumnWithNonexistentColumn() throws IOException {
        String csvData = "Name,Age\n" +
                         "Bob,25";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())
                .mapColumn("Height", val -> val.toUpperCase());

        // Attempt to map a nonexistent column
        assertThrows(NullPointerException.class, () -> {
                    csvStream.mapColumn("Height", val -> val.toUpperCase())
                            .toList();
        });
    }

    @Test
    void testDeleteColumnsWithNonexistentColumn() throws IOException {
        String csvData = "Name,Age\n" +
                         "Charlie,35";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Attempt to delete a nonexistent column
        csvStream = csvStream.deleteColumns("Country");
        List<org.yexey.common.util.csv.imp.Record> records = csvStream.toList();

        org.yexey.common.util.csv.imp.Record record = records.get(0);
        // Original columns should remain unaffected
        assertEquals("Charlie", record.get("Name"));
        assertEquals("35", record.get("Age"));
    }

    @Test
    void testFilterWithNonexistentColumn() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30\n" +
                         "Bob,25";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Attempt to filter on a nonexistent column
        csvStream = csvStream.filter("Country", val -> "USA".equals(val));
        List<org.yexey.common.util.csv.imp.Record> records = csvStream.toList();

        // Since "Country" doesn't exist, all records should be filtered out
        assertEquals(0, records.size());
    }

    @Test
    void testValidateWithNonexistentColumn() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Attempt to validate a nonexistent column
        csvStream = csvStream.validate("Country", val -> val != null, "Country is required");

        // Process the stream to trigger validation
        csvStream.consume();

        List<ValidationError> errors = csvStream.getValidationErrors();
        // Validation should fail for the nonexistent column
        assertEquals(1, errors.size());
        ValidationError error = errors.get(0);
        assertEquals("Country is required", error.getMessage());
        assertEquals("Alice", error.getRecord().get("Name"));
    }

    @Test
    void testJoinWithMissingKeyColumn() throws IOException {
        String csvDataA = "ID,Name\n" +
                          "1,Alice\n" +
                          "2,Bob";
        StringReader readerA = new StringReader(csvDataA);
        CSVStream csvStreamA = CSVStream.toCSVStream(readerA, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        String csvDataB = "UserID,Score\n" +
                          "1,85\n" +
                          "2,90";
        StringReader readerB = new StringReader(csvDataB);
        CSVStream csvStreamB = CSVStream.toCSVStream(readerB, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Attempt to join on a key column that doesn't exist in one of the streams
        assertThrows(NullPointerException.class, () -> {
            CSVStream joinedStream = csvStreamA.join(csvStreamB, "ID", "User_ID");
            joinedStream.toList();
        });
    }

    @Test
    void testAddColumnWithNullFunction() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Attempt to add a column with a null function
        assertThrows(NullPointerException.class, () -> {
            csvStream.addColumn("Country", (Function<org.yexey.common.util.csv.imp.Record, String>) null)
                    .toList();
        });
    }

    @Test
    void testFillMissingValuesWithNonexistentColumn() throws IOException {
        String csvData = "Name,Age\n" +
                         "Bob,25";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Fill missing values for a nonexistent column
        csvStream = csvStream.fillMissingValues("Country", "Unknown");
        List<org.yexey.common.util.csv.imp.Record> records = csvStream.toList();

        org.yexey.common.util.csv.imp.Record record = records.get(0);
        // The nonexistent column should now be added with the default value
        assertEquals("Unknown", record.get("Country"));
    }

    @Test
    void testSortWithNonexistentColumn() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30\n" +
                         "Bob,25";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Attempt to sort by a nonexistent column
        assertThrows(NullPointerException.class, () -> {
            csvStream.sort("Country", String::compareTo).toList();
        });
    }

    @Test
    void testJoinWithEmptyStreams() throws IOException {
        String csvDataA = "";
        StringReader readerA = new StringReader(csvDataA);
        CSVStream csvStreamA = CSVStream.toCSVStream(readerA, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        String csvDataB = "";
        StringReader readerB = new StringReader(csvDataB);
        CSVStream csvStreamB = CSVStream.toCSVStream(readerB, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Perform join on empty streams
        CSVStream joinedStream = csvStreamA.join(csvStreamB, "ID");
        List<org.yexey.common.util.csv.imp.Record> records = joinedStream.toList();

        // The result should be empty
        assertEquals(0, records.size());
    }

    @Test
    void testLeftJoinWithEmptyLeftStream() throws IOException {
        String csvDataA = "";
        StringReader readerA = new StringReader(csvDataA);
        CSVStream csvStreamA = CSVStream.toCSVStream(readerA, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        String csvDataB = "ID,Score\n" +
                          "1,85";
        StringReader readerB = new StringReader(csvDataB);
        CSVStream csvStreamB = CSVStream.toCSVStream(readerB, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        CSVStream joinedStream = csvStreamA.leftJoin(csvStreamB, "ID");
        List<org.yexey.common.util.csv.imp.Record> records = joinedStream.toList();

        // The result should be empty since left stream is empty
        assertEquals(0, records.size());
    }

    @Test
    void testRightJoinWithEmptyRightStream() throws IOException {
        String csvDataA = "ID,Name\n" +
                          "1,Alice";
        StringReader readerA = new StringReader(csvDataA);
        CSVStream csvStreamA = CSVStream.toCSVStream(readerA, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        String csvDataB = "";
        StringReader readerB = new StringReader(csvDataB);
        CSVStream csvStreamB = CSVStream.toCSVStream(readerB, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        CSVStream joinedStream = csvStreamA.rightJoin(csvStreamB, "ID");
        List<org.yexey.common.util.csv.imp.Record> records = joinedStream.toList();

        // The result should be empty since right stream is empty
        assertEquals(0, records.size());
    }

    @Test
    void testFullJoinWithEmptyStreams() throws IOException {
        String csvDataA = "";
        StringReader readerA = new StringReader(csvDataA);
        CSVStream csvStreamA = CSVStream.toCSVStream(readerA, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        String csvDataB = "";
        StringReader readerB = new StringReader(csvDataB);
        CSVStream csvStreamB = CSVStream.toCSVStream(readerB, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        CSVStream joinedStream = csvStreamA.fullJoin(csvStreamB, "ID");
        List<org.yexey.common.util.csv.imp.Record> records = joinedStream.toList();

        // The result should be empty
        assertEquals(0, records.size());
    }

    @Test
    void testValidateWithNullPredicate() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Attempt to validate with a null predicate
        assertThrows(NullPointerException.class, () -> {
            csvStream.validate("Age", null, "Predicate cannot be null");
            csvStream.consume();
        });
    }

    @Test
    void testGroupByWithNullFunction() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Attempt to group by with a null function
        assertThrows(NullPointerException.class, () -> {
            csvStream.groupBy(null);
        });
    }

    @Test
    void testReduceWithNullAccumulator() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Attempt to reduce with a null accumulator
        assertThrows(NullPointerException.class, () -> {
            csvStream.reduce(new org.yexey.common.util.csv.imp.Record(), null);
        });
    }

    @Test
    void testConsumeOnEmptyStream() throws IOException {
        String csvData = "";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Consume the empty stream
        csvStream.consume();
        // No exception should be thrown
    }

    @Test
    void testNullInputToCSVStream() {
        // Attempt to create a CSVStream with a null reader
        assertThrows(NullPointerException.class, () -> {
            CSVStream.toCSVStream(null, CSVFormat.DEFAULT.withFirstRecordAsHeader());
        });
    }

    @Test
    void testMapIntoWithNullFunction() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Attempt to map with a null function
        assertThrows(NullPointerException.class, () -> {
            csvStream.map(null).collect(Collectors.toList());
        });
    }

    @Test
    void testPrintColumnsWithNonexistentColumns() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Attempt to print columns that don't exist
        csvStream = csvStream.printColumns("Name", "Country");
        List<org.yexey.common.util.csv.imp.Record> records = csvStream.toList();

        // Should not throw exception, but "Country" column would be null
        Record record = records.get(0);
        assertEquals("Alice", record.get("Name"));
        assertNull(record.get("Country"));
    }

    @Test
    void testCopyOnConsumedStream() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        csvStream.consume(); // Consume the stream

        assertThrows(IllegalStateException.class, () -> {
            csvStream.copy();
        });
    }
}
