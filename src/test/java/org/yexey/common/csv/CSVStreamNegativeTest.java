package org.yexey.common.csv;

import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Test;
import org.yexey.common.csv.imp.Record;
import org.yexey.common.csv.imp.exceptions.ColumnNotFoundException;

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

        List<Record> records = csvStream.toList();
        assertEquals(0, records.size());
    }

    @Test
    void testRenameWithNonexistentColumn() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Attempt to rename a column that doesn't exist
        assertThrows(ColumnNotFoundException.class, () -> {
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
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Attempt to map a nonexistent column
        assertThrows(ColumnNotFoundException.class, () -> {
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
        List<Record> records = csvStream.toList();

        Record record = records.get(0);
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
        assertThrows(ColumnNotFoundException.class, () -> {
            csvStream.filter("Country", val -> "USA".equals(val))
                    .toList();
        });
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
        assertThrows(ColumnNotFoundException.class, () -> {
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
            csvStream.addColumn("Country", (Function<Record, String>) null)
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
        assertThrows(ColumnNotFoundException.class, () -> {
            csvStream.fillMissingValues("Country", "Unknown")
                            .toList();
        });
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
        List<Record> records = joinedStream.toList();

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
        List<Record> records = joinedStream.toList();

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
        List<Record> records = joinedStream.toList();

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
        List<Record> records = joinedStream.toList();

        // The result should be empty
        assertEquals(0, records.size());
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
            csvStream.reduce(new Record(), null);
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
        List<Record> records = csvStream.toList();

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
