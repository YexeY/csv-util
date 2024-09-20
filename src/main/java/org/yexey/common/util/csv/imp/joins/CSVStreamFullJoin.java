package org.yexey.common.util.csv.imp.joins;

import org.yexey.common.util.csv.Record;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVStreamFullJoin {
    public static Stream<Record> fullJoin(
            Stream<Record> streamA,
            Stream<Record> streamB,
            String keyColumnA,
            String keyColumnB
    ) {
        // Collect streamA and streamB into Maps
        Map<String, List<Record>> mapA = streamA
                .filter(record -> record.get(keyColumnA) != null)
                .collect(Collectors.groupingBy(record -> record.get(keyColumnA)));

        Map<String, List<Record>> mapB = streamB
                .filter(record -> record.get(keyColumnB) != null)
                .collect(Collectors.groupingBy(record -> record.get(keyColumnB)));

        // Create a set of all keys
        Set<String> allKeys = new HashSet<>();
        allKeys.addAll(mapA.keySet());
        allKeys.addAll(mapB.keySet());

        // Perform the full join
        return allKeys.stream().flatMap(key -> {
            List<Record> recordsA = mapA.getOrDefault(key, Arrays.asList((Record) null));
            List<Record> recordsB = mapB.getOrDefault(key, Arrays.asList((Record) null));

            return recordsA.stream().flatMap(recordA ->
                    recordsB.stream().map(recordB -> mergeRecords(recordA, recordB, keyColumnB))
            );
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
