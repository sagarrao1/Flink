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

public class WordCountCountWindow {

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
		
		DataStream<WordCount> wordCountStream = dataStream
							.flatMap(new WordCountSpiltter()) // this class returns Java Object of WordCount instead of Tuple. that is the difference.
							.keyBy("word") // it take string instead of index. because result of flatMap produces java object not Tuple
							.countWindow(3)
							.sum("count");		

		wordCountStream.print();
		env.execute("Count Window");
	}

	// this class returns Java Object of WordCount instead of Tuple. that is the difference.
	public static class WordCountSpiltter 
	           implements FlatMapFunction<String, WordCount> {

		public void flatMap(String sentence, Collector<WordCount> out) throws Exception {
//			System.out.println("In WordCountSpiltter class");
			for (String word : sentence.split(" ")) {
//				System.out.println(word);
				out.collect(new WordCount(word,1));
			}			
		}		
	}
	
//	this is POJO
	public static class WordCount{		
		public String word;
		public Integer count;
		
		public WordCount() {
		}

		public WordCount(String word, Integer count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return "WordCount [word=" + word + ", count=" + count + "]";
		}
		
		
		
	}

}
