package org.yexey.common.util.csv;

import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Test;
import org.yexey.common.util.csv.imp.Record;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CSVStreamCopyTest {

    @Test
    void testCopy_ShallowCopy() throws IOException {
        String csvData = "Name,Age,Country\n" +
                         "Alice,30,USA\n" +
                         "Bob,25,UK";
        StringReader reader = new StringReader(csvData);
        CSVStream originalStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Perform a shallow copy
        CSVStream copiedStream = originalStream.copy();

        // Modify a record in the copied stream
        copiedStream = copiedStream.mapColumn("Age", age -> String.valueOf(Integer.parseInt(age) + 5));

        // Collect records from both streams
        List<Record> originalRecords = originalStream.toList();
        List<Record> copiedRecords = copiedStream.toList();

        // Assertions
        // Original stream records should reflect the changes made in the copied stream
        assertEquals("35", originalRecords.get(0).get("Age")); // Alice's age increased from 30 to 35
        assertEquals("30", originalRecords.get(1).get("Age")); // Bob's age increased from 25 to 30

        // Copied stream records should have the same changes
        assertEquals("35", copiedRecords.get(0).get("Age"));   // Alice's age increased from 30 to 35
        assertEquals("30", copiedRecords.get(1).get("Age"));   // Bob's age increased from 25 to 30
    }

    @Test
    void testDeepCopy_DeepCopy() throws IOException {
        String csvData = "Name,Age,Country\n" +
                         "Alice,30,USA\n" +
                         "Bob,25,UK";
        StringReader reader = new StringReader(csvData);
        CSVStream originalStream = CSVStream.toCSVStream(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        // Perform a deep copy
        CSVStream deepCopiedStream = originalStream.deepCopy();

        // Modify a record in the deep-copied stream
        deepCopiedStream = deepCopiedStream.mapColumn("Age", age -> String.valueOf(Integer.parseInt(age) + 5));

        // Collect records from both streams
        List<Record> originalRecords = originalStream.toList();
        List<Record> deepCopiedRecords = deepCopiedStream.toList();

        // Assertions
        // Original stream records should not be affected by changes in the deep-copied stream
        assertEquals("30", originalRecords.get(0).get("Age")); // Alice's age remains 30
        assertEquals("25", originalRecords.get(1).get("Age")); // Bob's age remains 25

        // Deep-copied stream records should reflect the changes
        assertEquals("35", deepCopiedRecords.get(0).get("Age"));   // Alice's age increased from 30 to 35
        assertEquals("30", deepCopiedRecords.get(1).get("Age"));   // Bob's age increased from 25 to 30
    }
}
