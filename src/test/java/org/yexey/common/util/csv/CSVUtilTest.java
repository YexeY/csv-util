package org.yexey.common.util.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Test;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;

class CSVUtilTest {


    @Test
    public void readCSV() throws IOException {
        CSVStream csvStream = getCsvStreamForFileName("access-code.csv");

//        csvStream = csvStream
//                .addColumn("Full name", (elm) -> elm.get("First name") + " " + elm.get("Last name"))
//                .deleteColumns("First name", "Last name")
//                .rename("Full name", "Name")
//                .mapColumn("Name", String::toLowerCase)
//                .mapColumn("Name", elm -> elm.substring(0, 1).toUpperCase())
//                .print()
//                .filter("Name", (elm) -> elm.startsWith("R"));

        csvStream = csvStream.printAsTable();

        CSVStream tmp = CSVReader.fromFile("src/test/resources/access-code-password-recovery-code.csv", ';' ,StandardCharsets.UTF_8)
                        .printAsTable();

        csvStream.join(tmp, "Identifier", "Identifier")
                .printAsTable();

    }

    private static CSVStream getCsvStreamForFileName(String fileName) throws IOException {
        Reader in = new FileReader("src/test/resources/" + fileName);

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setDelimiter(';')
                .setHeader()
                .setSkipHeaderRecord(true)
                .build();
        CSVParser csvParser = new CSVParser(in, csvFormat);
        csvParser.iterator();
        List<CSVRecord> records = csvParser.getRecords();
        CSVStream cs = CSVStream.ofCSVRecords(records);
        return cs;
    }
}