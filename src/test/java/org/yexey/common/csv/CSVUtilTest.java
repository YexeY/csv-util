package org.yexey.common.csv;

import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Test;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

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

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setDelimiter(';')
                .setHeader()
                .setSkipHeaderRecord(true)
                .build();
        CSVStream tmp = CSVStream.toCSVStream(new FileReader("src/test/resources/access-code-password-recovery-code.csv"), csvFormat);
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
        CSVStream cs = CSVStream.toCSVStream(in, csvFormat);
        return cs;
    }
}