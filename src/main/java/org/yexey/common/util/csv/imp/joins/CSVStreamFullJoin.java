package org.yexey.common.util.csv.imp.joins;

import org.yexey.common.util.csv.imp.Record;

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
        // Collect streams into Maps
        Map<String, List<Record>> mapA = streamA
                .collect(Collectors.groupingBy(record -> record.get(keyColumnA)));

        Map<String, List<Record>> mapB = streamB
                .collect(Collectors.groupingBy(record -> record.get(keyColumnB)));

        // Create a set of all keys
        Set<String> allKeys = new HashSet<>();
        allKeys.addAll(mapA.keySet());
        allKeys.addAll(mapB.keySet());

        // Perform the full join
        return allKeys.stream().flatMap(key -> {
            List<Record> recordsA = mapA.getOrDefault(key, Collections.singletonList(null));
            List<Record> recordsB = mapB.getOrDefault(key, Collections.singletonList(null));

            return recordsA.stream().flatMap(recordA ->
                    recordsB.stream().map(recordB -> mergeRecords(recordA, recordB, keyColumnA, keyColumnB))
            );
        });
    }

    private static Record mergeRecords(Record recordA, Record recordB, String keyColumnA, String keyColumnB) {
        Record mergedRecord = new Record();

        // Merge data from recordA
        if (recordA != null) {
            mergedRecord.getData().putAll(recordA.getData());
        }

        // Merge data from recordB, avoiding overwriting existing keys
        if (recordB != null) {
            for (Map.Entry<String, String> entry : recordB.getData().entrySet()) {
                String key = entry.getKey();
                if (!key.equals(keyColumnB) && !mergedRecord.getData().containsKey(key)) {
                    mergedRecord.set(key, entry.getValue());
                }
            }
        }

        // Ensure the key column is included
        String keyValue = recordA != null ? recordA.get(keyColumnA) : recordB != null ? recordB.get(keyColumnB) : null;
        mergedRecord.set(keyColumnA, keyValue);

        return mergedRecord;
    }
}
