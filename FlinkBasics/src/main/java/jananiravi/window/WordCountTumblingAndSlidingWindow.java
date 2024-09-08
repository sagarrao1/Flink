package jananiravi.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import jananiravi.stream.StreamUtil;

public class WordCountTumblingAndSlidingWindow {

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env 
						= StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params);
		DataStream<String> dataStream = StreamUtil.getDataStream(env, params);
		
		if (dataStream == null) {
			System.exit(1);
			return;
		}
		
		DataStream<Tuple2<String, Integer>> wordCountStream = dataStream
							.flatMap(new WordCountSpiltter())
							.keyBy(0)
							.window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
							.sum(1);		

		wordCountStream.print();
		env.execute("WordCount Tumbling Window");
	}
	
	public static class WordCountSpiltter 
	           implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
//			System.out.println("In WordCountSpiltter class");
			for (String word : sentence.split(" ")) {
//				System.out.println(word);
				out.collect(new Tuple2<String, Integer>(word,1));
			}			
		}		
	}	

}
