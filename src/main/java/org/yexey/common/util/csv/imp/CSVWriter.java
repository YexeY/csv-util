package org.yexey.common.util.csv.imp;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class CSVWriter {

    private final Stream<Record> stream;

    private CSVWriter(Stream<Record> stream) {
        this.stream = stream;
    }

    public void writeTo(Writer writer, CSVFormat csvFormat) throws IOException {
        try (CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat)) {
            // Get headers from CSVFormat if present
            String[] headers = csvFormat.getHeader();

            // If headers are not specified, attempt to determine headers from the first record
            Iterator<Record> iterator = stream.iterator();
            if (!iterator.hasNext()) {
                // Stream is empty, nothing to write
                return;
            }

            Record firstRecord = iterator.next();

            if (headers == null || headers.length == 0) {
                // Determine headers from the first record
                Set<String> headerSet = firstRecord.getColumnNames();
                headers = headerSet.toArray(new String[0]);
                // Write headers to the output
                csvPrinter.printRecord((Object[]) headers);
            }

            // Write the first record
            writeRecord(csvPrinter, firstRecord, headers);

            // Write the rest of the records
            while (iterator.hasNext()) {
                Record record = iterator.next();
                writeRecord(csvPrinter, record, headers);
            }
        }
    }
    private void writeRecord(CSVPrinter csvPrinter, Record record, String[] headers) throws IOException {
        List<String> values = new ArrayList<>();
        for (String header : headers) {
            values.add(record.get(header));
        }
        csvPrinter.printRecord(values);
    }
}
