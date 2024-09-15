package com.janani.dataset;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;

public class MovieSelection {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple8<String, String, Integer, Integer, String, Long, String, String>> records
                = env.readCsvFile("src/main/resources/movie_metadata.csv")
                .ignoreFirstLine()
//                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(String.class, String.class, Integer.class, Integer.class,
                        String.class, Long.class, String.class, String.class);

        records.filter((FilterFunction<Tuple8<String, String, Integer, Integer, String, Long, String, String>>)
                movie -> movie.f1.contains("James Cameron"))
                .print();

//        Writing to file (sink)
//        records.filter((FilterFunction<Tuple8<String, String, Integer, Integer, String, Long, String, String>>)
////                movie -> movie.f1.equals("Black and White") /*&& movie.f7.contains("Romance")*/)
//                movie -> movie.f1.contains("James Cameron"))
//                .writeAsCsv("src/main/resources/sink", FileSystem.WriteMode.OVERWRITE);
//        env.execute();
    }


}
