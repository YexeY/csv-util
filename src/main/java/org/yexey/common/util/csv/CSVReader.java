package org.yexey.common.util.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CSVReader {

    private CSVReader() {
    }

    public static CSVStream toCSVStream(Reader reader, CSVFormat csvFormat) throws IOException {
        CSVParser csvParser = new CSVParser(reader, csvFormat);
        Stream<CSVRecord> recordIterator = csvParser.stream();
        return new CSVStream(recordIterator.map(Record::new));
    }

    public static CSVStream fromFile(String filePath, char delimiter, Charset charset) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), charset));
        return fromReader(reader, delimiter);
    }

    // Overloaded method with default delimiter ',' and default charset (UTF-8)
    public static CSVStream fromFile(String filePath) throws IOException {
        return fromFile(filePath, ',', StandardCharsets.UTF_8);
    }

    // Method to read from a BufferedReader with custom delimiter
    public static CSVStream fromReader(BufferedReader reader, char delimiter) throws IOException {
        // Read the header line
        String headerLine = reader.readLine();
        if (headerLine == null) {
            throw new IOException("Empty CSV file");
        }

        String[] headers = parseLine(headerLine, delimiter);

        // Read the rest of the lines and map them to Records
        Stream<Record> recordStream = reader.lines()
                .map(line -> parseLine(line, delimiter))
                .map(values -> {
                    Map<String, String> data = new HashMap<>();
                    for (int i = 0; i < headers.length && i < values.length; i++) {
                        data.put(headers[i], values[i]);
                    }
                    return new Record(data);
                });

        return new CSVStream(recordStream);
    }

    // Method to read from a BufferedReader with custom delimiter
    public static CSVStream fromReaderSplit(BufferedReader reader, char delimiter) throws IOException {

        // Read the header line
        String headerLine = reader.readLine();
        if (headerLine == null) {
            throw new IOException("Empty CSV file");
        }

        String[] headers = parseLine(headerLine, delimiter);

        // Read the rest of the lines and map them to Records
        // Create a custom Spliterator to process lines one at a time
        Spliterator<Record> spliterator = new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE, Spliterator.ORDERED) {
            @Override
            public boolean tryAdvance(Consumer<? super Record> action) {
                try {
                    String line = reader.readLine();
                    if (line == null) {
                        return false; // No more lines
                    }
                    String[] values = parseLine(line, delimiter);
                    Map<String, String> data = new HashMap<>();
                    for (int i = 0; i < headers.length && i < values.length; i++) {
                        data.put(headers[i], values[i]);
                    }
                    Record record = new Record(data);
                    action.accept(record);
                    return true;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };

        Stream<Record> recordStream = StreamSupport.stream(spliterator, false);

        return new CSVStream(recordStream);
    }

    // Overloaded method with default delimiter ','
    public static CSVStream fromReader(BufferedReader reader) throws IOException {
        return fromReader(reader, ',');
    }

    // Parsing a line with custom delimiter
    private static String[] parseLine(String line, char delimiter) {
        List<String> tokens = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = '"';

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == quoteChar) {
                inQuotes = !inQuotes;
            } else if (c == delimiter && !inQuotes) {
                tokens.add(currentToken.toString());
                currentToken.setLength(0);
            } else {
                currentToken.append(c);
            }
        }

        tokens.add(currentToken.toString());
        return tokens.toArray(new String[0]);
    }


}
