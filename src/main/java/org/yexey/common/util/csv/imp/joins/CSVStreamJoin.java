package org.yexey.common.util.csv.imp.joins;

import org.yexey.common.util.csv.imp.Record;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVStreamJoin {

    /**
     * INNER JOIN:
     * When you only want records that have matching values in both tables.
     * Example: Finding customers who have placed orders.
     */
    public static Stream<Record> join(Stream<Record> streamA, Stream<Record> streamB, String keyColumnA, String keyColumnB) {
        // Collect streamB into a Map from key to List of Records
        Map<String, List<Record>> mapB = streamB
                .filter(record -> {
                    if(!record.containsColumn(keyColumnB)) {
                        throw new NullPointerException("KeyColumn " + keyColumnB + " does not exists in StreamB");
                    }
                    return record.get(keyColumnB) != null;
                })
                .collect(Collectors.groupingBy(record -> record.get(keyColumnB)));

        // Perform the inner join
        return streamA
                .filter(recordA -> {
                    if(!recordA.containsColumn(keyColumnA)) {
                        throw new NullPointerException("KeyColumn " + keyColumnA + " does not exists in StreamA");
                    }
                    return recordA.get(keyColumnA) != null;
                })
                .flatMap(recordA -> {
                    String keyA = recordA.get(keyColumnA);
                    List<Record> matchingRecordsB = mapB.get(keyA);

                    if (matchingRecordsB != null) {
                        // Combine recordA with each matching recordB
                        return matchingRecordsB.stream()
                                .map(recordB -> mergeRecords(recordA, recordB, keyColumnB));
                    } else {
                        // No match found; exclude recordA from the result
                        return Stream.empty();
                    }
                });
    }

    // Helper method to merge two records
    private static Record mergeRecords(Record recordA, Record recordB, String keyColumnB) {
        Record mergedRecord = new Record();
        mergedRecord.getData().putAll(recordA.getData());

        // Add data from recordB, avoiding key conflicts
        if (recordB != null) {
            for (Map.Entry<String, String> entry : recordB.getData().entrySet()) {
                String key = entry.getKey();
                if (!key.equals(keyColumnB) && !mergedRecord.getData().containsKey(key)) {
                    mergedRecord.set(key, entry.getValue());
                }
            }
        }

        return mergedRecord;
    }
}
