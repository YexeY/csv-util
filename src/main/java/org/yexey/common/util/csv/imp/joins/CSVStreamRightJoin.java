package org.yexey.common.util.csv.imp.joins;

import org.yexey.common.util.csv.imp.Record;

import java.util.stream.Stream;

public class CSVStreamRightJoin {
    public static Stream<Record> rightJoin(Stream<Record> streamA, Stream<Record> streamB, String keyColumnA, String keyColumnB) {
       return CSVStreamLeftJoin.leftJoin(streamB, streamA, keyColumnB, keyColumnA);
    }
}