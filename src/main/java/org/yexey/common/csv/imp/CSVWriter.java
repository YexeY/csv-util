package org.yexey.common.csv.imp;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class CSVWriter {

    public static void writeTo(Stream<Record> stream, Writer writer, CSVFormat csvFormat) throws IOException {
        // Collect records to a list to allow multiple iterations
        List<Record> records = stream.toList();

        // Determine headers
        String[] headers = csvFormat.getHeader();
        if ((headers == null || headers.length == 0) && !records.isEmpty()) {
            // Get headers from the first record
            Set<String> headerSet = records.getFirst().getColumnNames();
            headers = headerSet.toArray(new String[0]);
            // Update CSVFormat with headers
            csvFormat = csvFormat.builder().setHeader(headers).build();
        }


        // Create CSVPrinter with updated CSVFormat
        try (CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat)) {
            // For each record, write the values in the order of headers
            for (Record record : records) {
                List<String> values = new ArrayList<>();
                for (String header : headers) {
                    values.add(record.get(header));
                }
                csvPrinter.printRecord(values);
            }
        }
    }
}
