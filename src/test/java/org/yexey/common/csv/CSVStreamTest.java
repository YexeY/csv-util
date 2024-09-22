package org.yexey.common.csv;

import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Test;
import org.yexey.common.csv.imp.Record;
import org.yexey.common.csv.imp.ValidationError;
import org.yexey.common.csv.imp.exceptions.ColumnAlreadyExistsException;

import java.io.*;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class CSVStreamTest {

    @Test
    void testToCSVStream() throws IOException {
        String csvData = "Name,Age,Country\n" +
                         "Alice,30,USA\n" +
                         "Bob,25,UK\n" +
                         "Charlie,35,Canada";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        List<Record> records = csvStream.toList();
        assertEquals(3, records.size());

        Record firstRecord = records.get(0);
        assertEquals("Alice", firstRecord.get("Name"));
        assertEquals("30", firstRecord.get("Age"));
        assertEquals("USA", firstRecord.get("Country"));
    }

    @Test
    void testRename() throws IOException {
        String csvData = "Name,Age,Country\n" +
                         "Alice,30,USA\n" +
                         "Bob,25,UK\n" +
                         "Charlie,35,Canada";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        csvStream = csvStream.rename("Age", "Years");
        List<Record> records = csvStream.toList();
        assertEquals(3, records.size());

        Record firstRecord = records.get(0);
        assertEquals("Alice", firstRecord.get("Name"));
        assertEquals("30", firstRecord.get("Years"));
        assertEquals("USA", firstRecord.get("Country"));
        assertNull(firstRecord.get("Age"));
    }

    @Test
    void testMapColumn() throws IOException {
        String csvData = "Name,Age,Country\n" +
                         "Alice,30,USA\n" +
                         "Bob,25,UK\n" +
                         "Charlie,35,Canada";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        csvStream = csvStream.mapColumn("Age", ageStr -> {
            int age = Integer.parseInt(ageStr);
            return String.valueOf(age + 1);
        });
        List<Record> records = csvStream.toList();
        assertEquals(3, records.size());

        Record firstRecord = records.get(0);
        assertEquals("31", firstRecord.get("Age"));
    }

    @Test
    void testDeleteColumns() throws IOException {
        String csvData = "Name,Age,Country\n" +
                         "Alice,30,USA\n" +
                         "Bob,25,UK\n" +
                         "Charlie,35,Canada";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        csvStream = csvStream.deleteColumns("Country");
        List<Record> records = csvStream.toList();
        assertEquals(3, records.size());

        Record firstRecord = records.get(0);
        assertEquals("Alice", firstRecord.get("Name"));
        assertEquals("30", firstRecord.get("Age"));
        assertNull(firstRecord.get("Country"));
    }

    @Test
    void testRetainColumn() throws IOException {
        String csvData = "Name,Age,Country\n" +
                         "Alice,30,USA\n" +
                         "Bob,25,UK\n" +
                         "Charlie,35,Canada";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        csvStream = csvStream.retainColumn("Name", "Country");
        List<Record> records = csvStream.toList();
        assertEquals(3, records.size());

        Record firstRecord = records.get(0);
        assertEquals("Alice", firstRecord.get("Name"));
        assertEquals("USA", firstRecord.get("Country"));
        assertNull(firstRecord.get("Age"));
    }

    @Test
    void testFilter() throws IOException {
        String csvData = "Name,Age,Country\n" +
                         "Alice,30,USA\n" +
                         "Bob,25,UK\n" +
                         "Charlie,35,Canada";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        csvStream = csvStream.filter("Age", ageStr -> Integer.parseInt(ageStr) > 30);
        List<Record> records = csvStream.toList();
        assertEquals(1, records.size());

        Record firstRecord = records.get(0);
        assertEquals("Charlie", firstRecord.get("Name"));
        assertEquals("35", firstRecord.get("Age"));
    }

    @Test
    void testAddColumn() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,17\n" +
                         "Bob,25\n" +
                         "Charlie,15";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        csvStream = csvStream.addColumn("AgeGroup", record -> {
            int age = Integer.parseInt(record.get("Age"));
            return age >= 18 ? "Adult" : "Minor";
        });
        List<Record> records = csvStream.toList();
        assertEquals(3, records.size());

        Record firstRecord = records.get(0);
        assertEquals("Minor", firstRecord.get("AgeGroup"));

        Record secondRecord = records.get(1);
        assertEquals("Adult", secondRecord.get("AgeGroup"));
    }

    @Test
    void testFillMissingValues() throws IOException {
        String csvData = "Name,Age,Country\n" +
                         "Alice,30,USA\n" +
                         "Bob,25,\n" +
                         "Charlie,35,Canada";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        csvStream = csvStream.fillMissingValues("Country", "Unknown");
        List<Record> records = csvStream.toList();
        assertEquals(3, records.size());

        Record secondRecord = records.get(1);
        assertEquals("Unknown", secondRecord.get("Country"));
    }

    @Test
    void testSort() throws IOException {
        String csvData = "Name,Age,Country\n" +
                         "Alice,30,USA\n" +
                         "Bob,25,UK\n" +
                         "Charlie,35,Canada";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        csvStream = csvStream.sort("Age", Comparator.comparingInt(Integer::parseInt));
        List<Record> records = csvStream.toList();
        assertEquals(3, records.size());

        assertEquals("Bob", records.get(0).get("Name"));      // Age 25
        assertEquals("Alice", records.get(1).get("Name"));    // Age 30
        assertEquals("Charlie", records.get(2).get("Name"));  // Age 35
    }

    @Test
    void testJoin() throws IOException {
        String csvDataA = "ID,Name\n" +
                          "1,Alice\n" +
                          "2,Bob\n" +
                          "3,Charlie";
        StringReader readerA = new StringReader(csvDataA);
        CSVStream csvStreamA = CSVStream.toCSVStream(readerA, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        String csvDataB = "ID,Score\n" +
                          "1,85\n" +
                          "2,90\n" +
                          "4,75";
        StringReader readerB = new StringReader(csvDataB);
        CSVStream csvStreamB = CSVStream.toCSVStream(readerB, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        CSVStream joinedStream = csvStreamA.join(csvStreamB, "ID");
        List<Record> records = joinedStream.toList();
        assertEquals(2, records.size());

        Record firstRecord = records.get(0);
        assertEquals("1", firstRecord.get("ID"));
        assertEquals("Alice", firstRecord.get("Name"));
        assertEquals("85", firstRecord.get("Score"));

        Record secondRecord = records.get(1);
        assertEquals("2", secondRecord.get("ID"));
        assertEquals("Bob", secondRecord.get("Name"));
        assertEquals("90", secondRecord.get("Score"));
    }

    @Test
    void testLeftJoin() throws IOException {
        String csvDataA = "ID,Name\n" +
                          "1,Alice\n" +
                          "2,Bob\n" +
                          "3,Charlie";
        StringReader readerA = new StringReader(csvDataA);
        CSVStream csvStreamA = CSVStream.toCSVStream(readerA, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        String csvDataB = "ID,Score\n" +
                          "1,85\n" +
                          "2,90\n" +
                          "4,75";
        StringReader readerB = new StringReader(csvDataB);
        CSVStream csvStreamB = CSVStream.toCSVStream(readerB, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        CSVStream joinedStream = csvStreamA.leftJoin(csvStreamB, "ID");
        List<Record> records = joinedStream.toList();
        assertEquals(3, records.size());

        Record firstRecord = records.get(0);
        assertEquals("1", firstRecord.get("ID"));
        assertEquals("Alice", firstRecord.get("Name"));
        assertEquals("85", firstRecord.get("Score"));

        Record secondRecord = records.get(1);
        assertEquals("2", secondRecord.get("ID"));
        assertEquals("Bob", secondRecord.get("Name"));
        assertEquals("90", secondRecord.get("Score"));

        Record thirdRecord = records.get(2);
        assertEquals("3", thirdRecord.get("ID"));
        assertEquals("Charlie", thirdRecord.get("Name"));
        assertNull(thirdRecord.get("Score"));
    }

    @Test
    void testRightJoin() throws IOException {
        String csvDataA = "ID,Name\n" +
                          "1,Alice\n" +
                          "2,Bob\n" +
                          "3,Charlie";
        StringReader readerA = new StringReader(csvDataA);
        CSVStream csvStreamA = CSVStream.toCSVStream(readerA, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        String csvDataB = "ID,Score\n" +
                          "1,85\n" +
                          "2,90\n" +
                          "4,75";
        StringReader readerB = new StringReader(csvDataB);
        CSVStream csvStreamB = CSVStream.toCSVStream(readerB, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        CSVStream joinedStream = csvStreamA.rightJoin(csvStreamB, "ID");
        List<Record> records = joinedStream.toList();
        assertEquals(3, records.size());

        Record firstRecord = records.stream().filter(r -> "1".equals(r.get("ID"))).findFirst().orElse(null);
        assertNotNull(firstRecord);
        assertEquals("Alice", firstRecord.get("Name"));
        assertEquals("85", firstRecord.get("Score"));

        Record secondRecord = records.stream().filter(r -> "2".equals(r.get("ID"))).findFirst().orElse(null);
        assertNotNull(secondRecord);
        assertEquals("Bob", secondRecord.get("Name"));
        assertEquals("90", secondRecord.get("Score"));

        Record thirdRecord = records.stream().filter(r -> "4".equals(r.get("ID"))).findFirst().orElse(null);
        assertNotNull(thirdRecord);
        assertNull(thirdRecord.get("Name"));
        assertEquals("75", thirdRecord.get("Score"));
    }

    @Test
    void testFullJoin() throws IOException {
        String csvDataA = "ID,Name\n" +
                          "1,Alice\n" +
                          "2,Bob\n" +
                          "3,Charlie";
        StringReader readerA = new StringReader(csvDataA);
        CSVStream csvStreamA = CSVStream.toCSVStream(readerA, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        String csvDataB = "ID,Score\n" +
                          "1,85\n" +
                          "2,90\n" +
                          "4,75";
        StringReader readerB = new StringReader(csvDataB);
        CSVStream csvStreamB = CSVStream.toCSVStream(readerB, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        CSVStream joinedStream = csvStreamA.fullJoin(csvStreamB, "ID");
        List<Record> records = joinedStream.toList();
        assertEquals(4, records.size());

        Record record1 = records.stream().filter(r -> "1".equals(r.get("ID"))).findFirst().orElse(null);
        assertNotNull(record1);
        assertEquals("Alice", record1.get("Name"));
        assertEquals("85", record1.get("Score"));

        Record record2 = records.stream().filter(r -> "2".equals(r.get("ID"))).findFirst().orElse(null);
        assertNotNull(record2);
        assertEquals("Bob", record2.get("Name"));
        assertEquals("90", record2.get("Score"));

        Record record3 = records.stream().filter(r -> "3".equals(r.get("ID"))).findFirst().orElse(null);
        assertNotNull(record3);
        assertEquals("Charlie", record3.get("Name"));
        assertNull(record3.get("Score"));

        Record record4 = records.stream().filter(r -> "4".equals(r.get("ID"))).findFirst().orElse(null);
        assertNotNull(record4);
        assertNull(record4.get("Name"));
        assertEquals("75", record4.get("Score"));
    }

    @Test
    void testGroupBy() throws IOException {
        String csvData = "Name,Age,Country\n" +
                         "Alice,30,USA\n" +
                         "Bob,25,UK\n" +
                         "Charlie,35,USA\n" +
                         "Dave,40,Canada";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        Map<String, List<Record>> grouped = csvStream.groupBy(record -> record.get("Country"));
        assertEquals(3, grouped.size());
        assertTrue(grouped.containsKey("USA"));
        assertTrue(grouped.containsKey("UK"));
        assertTrue(grouped.containsKey("Canada"));

        List<Record> usaRecords = grouped.get("USA");
        assertEquals(2, usaRecords.size());
        assertEquals("Alice", usaRecords.get(0).get("Name"));
        assertEquals("Charlie", usaRecords.get(1).get("Name"));
    }

    @Test
    void testReduce() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30\n" +
                         "Bob,25\n" +
                         "Charlie,35";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        Record identity = new Record();
        identity.put("AgeSum", "0");

        Record result = csvStream.reduce(identity, (record1, record2) -> {
            int sum = Integer.parseInt(record1.get("AgeSum")) + Integer.parseInt(record2.get("Age"));
            record1.put("AgeSum", String.valueOf(sum));
            return record1;
        });

        assertEquals("90", result.get("AgeSum"));
    }

    @Test
    void testMap() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30\n" +
                         "Bob,25\n" +
                         "Charlie,35";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        List<String> names = csvStream.map(record -> record.get("Name")).collect(Collectors.toList());
        assertEquals(3, names.size());
        assertEquals("Alice", names.get(0));
        assertEquals("Bob", names.get(1));
        assertEquals("Charlie", names.get(2));
    }

    @Test
    void testMultipleConsumeAndContinue() throws IOException {
        String csvData = "Name,Age\n" +
                "Alice,30\n" +
                "Bob,25";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // First consumption
        csvStream = csvStream.consumeAndContinue();

        // Second consumption
        csvStream = csvStream.consumeAndContinue();

        // Attempt to collect records after multiple consumptions
        List<Record> records = csvStream.toList();

        // Assertions
        // The records should still be available after multiple consumptions
        assertEquals(2, records.size());
        assertEquals("Alice", records.get(0).get("Name"));
        assertEquals("Bob", records.get(1).get("Name"));
    }

    @Test
    void testConsumeAndContinue() throws IOException {
        String csvData = "Name,Age\n" +
                "Alice,30\n" +
                "Bob,25\n" +
                "Charlie,35";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Add a peek operation to observe when records are processed
        csvStream = csvStream.peek(record -> System.out.println("Processing: " + record.get("Name")));

        // Consume the stream
        csvStream = csvStream.consumeAndContinue();

        // After consuming, we should be able to perform further operations
        csvStream = csvStream.filter(record -> Integer.parseInt(record.get("Age")) > 30);

        List<Record> records = csvStream.toList();

        // Assertions
        // Only "Charlie" should be in the filtered list
        assertEquals(1, records.size());
        assertEquals("Charlie", records.get(0).get("Name"));
    }


    @Test
    void testAddColumnWithStaticValue() throws IOException {
        String csvData = "Name,Age\n" +
                "Alice,30\n" +
                "Bob,25";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Add a new column "Country" with static value "Unknown"
        csvStream = csvStream.addColumn("Country", "Unknown");

        List<Record> records = csvStream.toList();

        // Assertions
        assertEquals(2, records.size());
        for (Record record : records) {
            assertEquals("Unknown", record.get("Country"));
        }
    }

    @Test
    void testAddColumnOverwriteExisting() throws IOException {
        String csvData = "Name,Age,Status\n" +
                "Alice,30,Active\n" +
                "Bob,25,Inactive";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Add a column "Status" with static value "Pending"
        assertThrows(ColumnAlreadyExistsException.class, () -> {
            csvStream.addColumn("Status", "Pending")
                    .toList();
        });
    }

    @Test
    void testSortByColumnAscending() throws IOException {
        String csvData = "Name,Age\n" +
                "Charlie,35\n" +
                "Alice,30\n" +
                "Bob,25";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Sort by "Name" in ascending order
        csvStream = csvStream.sort("Name", String::compareTo);

        List<Record> records = csvStream.toList();

        // Assertions
        assertEquals(3, records.size());
        assertEquals("Alice", records.get(0).get("Name"));
        assertEquals("Bob", records.get(1).get("Name"));
        assertEquals("Charlie", records.get(2).get("Name"));
    }

    @Test
    void testSortByColumnDescending() throws IOException {
        String csvData = "Name,Age\n" +
                "Charlie,35\n" +
                "Alice,30\n" +
                "Bob,25";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Sort by "Age" in descending order
        csvStream = csvStream.sort("Age", Comparator.comparingInt((String elm) -> Integer.parseInt(elm)).reversed());

        List<Record> records = csvStream.toList();

        // Assertions
        assertEquals(3, records.size());
        assertEquals("Charlie", records.get(0).get("Name")); // Age 35
        assertEquals("Alice", records.get(1).get("Name"));   // Age 30
        assertEquals("Bob", records.get(2).get("Name"));     // Age 25
    }

    @Test
    void testSortWithRecordComparator() throws IOException {
        String csvData = "Name,Age,Country\n" +
                "Charlie,35,Canada\n" +
                "Alice,30,USA\n" +
                "Bob,25,UK";
        StringReader reader = new StringReader(csvData);
        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Sort by "Country" length ascending
        csvStream = csvStream.sort(Comparator.comparingInt(record -> record.get("Country").length()));

        List<Record> records = csvStream.toList();

        // Assertions
        assertEquals(3, records.size());
        assertEquals("UK", records.get(0).get("Country"));        // Length 2
        assertEquals("USA", records.get(1).get("Country"));       // Length 3
        assertEquals("Canada", records.get(2).get("Country"));    // Length 6
    }

    @Test
    void testWriteTo_StringWriter() throws IOException {
        String csvData = "Name,Age,Country\n" +
                "Alice,30,USA\n" +
                "Bob,25,UK";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        StringWriter writer = new StringWriter();
        csvStream.writeTo(writer, CSVFormat.DEFAULT.withHeader("Name", "Age", "Country"));

        String expectedOutput = "Name,Age,Country\r\n" +
                "Alice,30,USA\r\n" +
                "Bob,25,UK\r\n";

        assertEquals(expectedOutput, writer.toString());
    }

    @Test
    void testWriteTo_EmptyStream() throws IOException {
        String csvData = "Name,Age,Country";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        StringWriter writer = new StringWriter();
        csvStream.writeTo(writer, CSVFormat.DEFAULT.withHeader("Name", "Age", "Country"));

        String expectedOutput = "Name,Age,Country\r\n";

        assertEquals(expectedOutput, writer.toString());
    }

    @Test
    void testWriteTo_CustomDelimiter() throws IOException {
        String csvData = "Name;Age;Country\n" +
                "Alice;30;USA\n" +
                "Bob;25;UK";
        StringReader reader = new StringReader(csvData);

        CSVFormat customFormat = CSVFormat.DEFAULT.withDelimiter(';').withFirstRecordAsHeader();
        CSVStream csvStream = CSVStream.toCSVStream(reader, customFormat);

        StringWriter writer = new StringWriter();
        csvStream.writeTo(writer, customFormat.DEFAULT.withHeader("Name", "Country").withDelimiter(';'));

        String expectedOutput = "Name;Country\r\n" +
                "Alice;USA\r\n" +
                "Bob;UK\r\n";

        assertEquals(expectedOutput, writer.toString());
    }

    @Test
    void testWriteTo_CustomDelimiter_All_Headers() throws IOException {
        String csvData = "Name;Age;Country\n" +
                "Alice;30;USA\n" +
                "Bob;25;UK";
        StringReader reader = new StringReader(csvData);

        CSVFormat customFormat = CSVFormat.DEFAULT.withDelimiter(';').withFirstRecordAsHeader();
        CSVStream csvStream = CSVStream.toCSVStream(reader, customFormat);

        StringWriter writer = new StringWriter();
        csvStream.writeTo(writer, customFormat.DEFAULT.withDelimiter(';'));

        String expectedOutput = "Name;Age;Country\r\n" +
                "Alice;30;USA\r\n" +
                "Bob;25;UK\r\n";

        assertEquals(expectedOutput, writer.toString());
    }

    @Test
    void testReduce_SumOfAges() throws IOException {
        String csvData = "Name,Age\n" +
                "Alice,30\n" +
                "Bob,25\n" +
                "Charlie,35";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        Optional<Record> result = csvStream.reduce((record1, record2) -> {
            int age1 = Integer.parseInt(record1.get("Age"));
            int age2 = Integer.parseInt(record2.get("Age"));
            Record newRecord = new Record();
            newRecord.put("Age", String.valueOf(age1 + age2));
            return newRecord;
        });

        assertTrue(result.isPresent());
        assertEquals("90", result.get().get("Age"));
    }

    @Test
    void testReduce_FindOldestPerson() throws IOException {
        String csvData = "Name,Age\n" +
                "Alice,30\n" +
                "Bob,45\n" +
                "Charlie,35";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        Optional<Record> oldestPerson = csvStream.reduce((record1, record2) -> {
            int age1 = Integer.parseInt(record1.get("Age"));
            int age2 = Integer.parseInt(record2.get("Age"));
            return age1 >= age2 ? record1 : record2;
        });

        assertTrue(oldestPerson.isPresent());
        assertEquals("Bob", oldestPerson.get().get("Name"));
    }

    @Test
    void testReduce_EmptyStream() throws IOException {
        String csvData = "Name,Age";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        Optional<Record> result = csvStream.reduce((record1, record2) -> record1);

        assertFalse(result.isPresent());
    }


    @Test
    void testPrintColumnsAsTable_PrintStream() throws IOException {
        String csvData = "Name,Age,Country\n" +
                "Alice,30,USA\n" +
                "Bob,25,UK";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Capture the output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        csvStream.printColumnsAsTable(ps, "Name", "Country");

        String expectedOutput = "Name   Country  \r\n" +
                                "-----  -------  \r\n" +
                                "Alice  USA      \r\n" +
                                "Bob    UK       \r\n";

        assertEquals(expectedOutput, baos.toString());
    }

    @Test
    void testPrintColumnsAsTable_NonexistentColumn() throws IOException {
        String csvData = "Name,Age\n" +
                "Alice,30\n" +
                "Bob,25";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Capture the output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        csvStream.printColumnsAsTable(ps, "Name", "Country");

        String expectedOutput = "Name   Country  \r\n" +
                                "-----  -------  \r\n" +
                                "Alice           \r\n" +
                                "Bob             \r\n";

        assertEquals(expectedOutput, baos.toString());
    }

    @Test
    void testPrint_CommaDelimiter() throws IOException {
        String csvData = "Name,Age\n" +
                "Alice,30\n" +
                "Bob,25";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        csvStream.print(ps, ',').consume();

        String expectedOutput = "Alice,30\r\n" +
                "Bob,25\r\n";

        assertEquals(expectedOutput, baos.toString());
    }

    @Test
    void testPrint_TabDelimiter() throws IOException {
        String csvData = "Name,Age\n" +
                "Charlie,35\n" +
                "David,40";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        csvStream.print(ps, '\t')
                .consume();

        String expectedOutput = "Charlie\t35\r\n" +
                                "David\t40\r\n";

        assertEquals(expectedOutput, baos.toString());
    }

    @Test
    void testPrint_EmptyStream() throws IOException {
        String csvData = "Name,Age";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        csvStream.print(ps, ',');

        String expectedOutput = "";

        assertEquals(expectedOutput, baos.toString());
    }

    @Test
    void testValidateAndThrowOnFailure_Column_Passes() throws IOException {
        String csvData = "Name,Age\n" +
                "Alice,30\n" +
                "Bob,35";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        csvStream = csvStream.validateAndThrowOnFailure("Age", ageStr -> Integer.parseInt(ageStr) >= 30, () -> new IllegalStateException("Age must be at least 30"));

        // Consume the stream without exception
        csvStream.consume();
    }

    @Test
    void testValidateAndThrowOnFailure_Column_Fails() throws IOException {
        String csvData = "Name,Age\n" +
                "Alice,25\n" +
                "Bob,35";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        csvStream = csvStream.validateAndThrowOnFailure("Age", ageStr -> Integer.parseInt(ageStr) >= 30, () -> new IllegalStateException("Age must be at least 30"));

        // Expect an exception when consuming the stream
        IllegalStateException exception = assertThrows(IllegalStateException.class, csvStream::consume);
        assertEquals("Age must be at least 30", exception.getMessage());
    }

    @Test
    void testValidateAndThrowOnFailure_Record_Passes() throws IOException {
        String csvData = "Name,Age,Country\n" +
                "Alice,30,USA\n" +
                "Bob,35,UK";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        Predicate<Record> validator = record -> "USA".equals(record.get("Country")) || "UK".equals(record.get("Country"));

        csvStream = csvStream.validateAndThrowOnFailure(validator, () -> new IllegalArgumentException("Invalid country"));

        // Consume the stream without exception
        csvStream.consume();
    }

    @Test
    void testValidateAndThrowOnFailure_Record_Fails() throws IOException {
        String csvData = "Name,Age,Country\n" +
                "Alice,30,USA\n" +
                "Charlie,40,Canada";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        Predicate<Record> validator = record -> "USA".equals(record.get("Country")) || "UK".equals(record.get("Country"));

        csvStream = csvStream.validateAndThrowOnFailure(validator, () -> new IllegalArgumentException("Invalid country"));

        // Expect an exception when consuming the stream
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, csvStream::consume);
        assertEquals("Invalid country", exception.getMessage());
    }

    @Test
    void testValidateAndThrowOnFailure_NullPredicate() throws IOException {
        String csvData = "Name,Age\n" +
                "Alice,30";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Null predicate should throw NullPointerException
        assertThrows(NullPointerException.class, () -> {
            csvStream.validateAndThrowOnFailure("Age", null, () -> new RuntimeException("Validation failed"));
        });

        // Null exception supplier should throw NullPointerException
        assertThrows(NullPointerException.class, () -> {
            csvStream.validateAndThrowOnFailure("Age", ageStr -> true, null);
        });
    }

    @Test
    void testValidateEager_Passes() throws IOException {
        String csvData = "Name,Age\n" +
                "Alice,30\n" +
                "Bob,35";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        List<ValidationError> errors = csvStream.validateEager("Age", ageStr -> Integer.parseInt(ageStr) >= 30, "Age must be at least 30");

        // No errors should be collected
        assertTrue(errors.isEmpty());
    }

    @Test
    void testValidateEager_Fails() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,25\n" +
                         "Bob,35\n" +
                         "Charlie,28";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        List<ValidationError> errors = csvStream.validateEager("Age", ageStr -> Integer.parseInt(ageStr) >= 30, "Age must be at least 30");

        // Two records should have validation errors
        assertEquals(2, errors.size());

        assertEquals("Alice", errors.get(0).getRecord().get("Name"));
        assertEquals("Age must be at least 30", errors.get(0).getMessage());

        assertEquals("Charlie", errors.get(1).getRecord().get("Name"));
        assertEquals("Age must be at least 30", errors.get(1).getMessage());
    }

    @Test
    void testValidateEager_NonexistentColumn() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Expect an exception when validating a non-existent column

        List<ValidationError> validation = csvStream.validateEager("Height", value -> true, "Height is required");

        assertFalse(validation.isEmpty());
    }

    @Test
    void testValidateEager_NullPredicate() throws IOException {
        String csvData = "Name,Age\n" +
                         "Alice,30";
        StringReader reader = new StringReader(csvData);

        CSVStream csvStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Null predicate should throw NullPointerException
        assertThrows(NullPointerException.class, () -> {
            csvStream.validateEager("Age", null, "Validator must not be null");
        });
    }
}
