package jananiravi.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import jananiravi.stream.StreamUtil;

public class ViewsSessionWindow {

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
		
		DataStream<Tuple3<String, String, Double>> sessionWindowStream = dataStream
							.map(new RowSpiltter())
							.keyBy(0,1)
							.window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
							.max(2);		

		sessionWindowStream.print();
		env.execute("Average views per user and per user , per page");
	}
	
	public static class RowSpiltter 
	           implements MapFunction<String, Tuple3<String,String, Double>> {

		@Override
		public Tuple3<String, String, Double> map(String input) throws Exception {
			String[] fields = input.split(",");
			if (fields.length ==3) {
				return new Tuple3<String, String, Double>(
						fields[0],  /*user id*/
						fields[1],  /*web page id*/
						Double.parseDouble(fields[2]) /*view time in minutes*/
						);
			} else return null;
		}		
	}		
		

}
