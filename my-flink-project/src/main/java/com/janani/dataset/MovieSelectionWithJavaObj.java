package com.janani.dataset;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple8;

public class MovieSelectionWithJavaObj {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple8<String, String, Integer, Integer, String, Long, String, String>> records
                = env.readCsvFile("src/main/resources/movie_metadata.csv")
                .ignoreFirstLine()
//                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(String.class, String.class, Integer.class, Integer.class,
                        String.class, Long.class, String.class, String.class);

            records.map(new ConvertToMovieObjects())
                    .filter( new FilterGross(18000l))
                    .print();

//        records.filter((FilterFunction<Tuple8<String, String, Integer, Integer, String, Long, String, String>>)
////                movie -> movie.f1.equals("Black and White") /*&& movie.f7.contains("Romance")*/)
//                movie -> movie.f1.contains("James Cameron"))
//                .writeAsCsv("src/main/resources/sink", FileSystem.WriteMode.OVERWRITE);
//            env.execute();
    }

    public static class ConvertToMovieObjects
            implements MapFunction<
            Tuple8<String, String, Integer, Integer, String, Long, String, String>, Movie > {
        @Override
        public Movie map(Tuple8<String, String, Integer, Integer, String, Long, String, String> row) throws Exception {
            return new Movie(row.f6, row.f1,row.f7,row.f5);
        }
    }

    public static class FilterGross implements FilterFunction<Movie> {
        private Long gross;

        public FilterGross(Long gross) {
            this.gross = gross;
        }

        @Override
        public boolean filter(Movie movie) throws Exception {
            return movie.getGross() > gross;
        }
    }


}
