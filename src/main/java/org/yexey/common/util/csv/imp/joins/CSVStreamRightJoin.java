package org.yexey.common.util.csv.imp.joins;

import org.yexey.common.util.csv.Record;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVStreamRightJoin {
    public static Stream<Record> rightJoin(Stream<Record> streamA, Stream<Record> streamB, String keyColumnA, String keyColumnB) {
        // Collect streamA into a Map from key to List of Records
        Map<String, List<Record>> mapA = streamA
                .filter(record -> record.get(keyColumnA) != null)
                .collect(Collectors.groupingBy(record -> record.get(keyColumnA)));

        // Perform the right join
        return streamB
                .filter(recordB -> recordB.get(keyColumnB) != null)
                .flatMap(recordB -> {
                    String keyB = recordB.get(keyColumnB);
                    List<Record> matchingRecordsA = mapA.get(keyB);

                    if (matchingRecordsA != null) {
                        // Combine recordB with each matching recordA
                        return matchingRecordsA.stream()
                                .map(recordA -> mergeRecords(recordA, recordB, keyColumnB));
                    } else {
                        // No match found; merge null (recordA) with recordB
                        return Stream.of(mergeRecords(null, recordB, keyColumnB));
                    }
                });
    }

    // Helper method to merge two records
    private static Record mergeRecords(Record recordA, Record recordB, String keyColumnB) {
        Record mergedRecord = new Record();

        if (recordA != null) {
            mergedRecord.getData().putAll(recordA.getData());
        }

        if (recordB != null) {
            for (Map.Entry<String, String> entry : recordB.getData().entrySet()) {
                String key = entry.getKey();
                if (!key.equals(keyColumnB) && (recordA == null || !mergedRecord.getData().containsKey(key))) {
                    mergedRecord.set(key, entry.getValue());
                }
            }
        }

        return mergedRecord;
    }
}
