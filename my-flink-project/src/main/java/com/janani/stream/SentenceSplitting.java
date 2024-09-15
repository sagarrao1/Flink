package com.janani.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SentenceSplitting {
 public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<String> dataStream = env.readTextFile("src/main/resources/carList.txt")
                .flatMap(new SentenceSplitter());

        dataStream.print();
        env.execute("Splitting words");

    }
    public static class SentenceSplitter implements FlatMapFunction<String,String> {

        @Override
        public void flatMap(String sentence, Collector<String> out) throws Exception {
            for (String word: sentence.split(",")){
                out.collect(word);
            }
        }
    }
}
