package com.janani.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.Map;

public class CourseScoreExtractor {
 public static void main(String[] args) throws Exception {
     final StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();

     DataStream<String> dataStream = env.readTextFile("src/main/resources/student_scores.txt");

     DataStream<Tuple3<String, String, Integer>> courseStream
             = dataStream.flatMap(new CourseScoreExtractorFn());

     courseStream.print();
     env.execute("Splitting words");

    }
    public static class CourseScoreExtractorFn
            implements FlatMapFunction<String, Tuple3<String,String,Integer>> {

      private static final Map<Integer,String > courseLookup= new HashMap<>();

      static {
          courseLookup.put(1,"Math");
          courseLookup.put(2,"Physics");
          courseLookup.put(3,"Chemistry");
          courseLookup.put(4,"English");
      }

        @Override
        public void flatMap(String row, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            String[] tokens = row.split(",");

            if(tokens.length <2){
                return;
            }
            for(Integer indexKey: courseLookup.keySet()){
                if(indexKey < tokens.length){
                    out.collect(new Tuple3<>(
                            tokens[0].trim(),
                            courseLookup.get(indexKey),
                            Integer.parseInt(tokens[indexKey].trim())
                    ));
                }
            }
        }
    }
}
