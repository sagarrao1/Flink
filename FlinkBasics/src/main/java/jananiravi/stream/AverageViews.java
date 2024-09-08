package jananiravi.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageViews {

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
		
		DataStream<Tuple2<String, Double>> averageViewStream = dataStream
							.map(new RowSplitter())
							.keyBy(0)
							.reduce(new SumAndCount())
							.map(new Average());

		averageViewStream.print();
		env.execute("Average Views in minutes per webpage Id");
	}
	
	public static class RowSplitter implements  
			MapFunction<String, Tuple3<String,Double,Integer >> {

		@Override
		public Tuple3<String, Double, Integer> map(String sentence) throws Exception {
			String[] split = sentence.split(",");
			if (split.length ==2) {
				return new Tuple3<String, Double, Integer>
				          (split[0], /* webpage_id */ 
						   Double.parseDouble(split[1]), /* view in minutes */
						    1 /* count */);
			}
			return null;
		}		
	}
	
	public static class SumAndCount implements 
			ReduceFunction<Tuple3<String, Double, Integer>>{

		@Override
		public Tuple3<String, Double, Integer> reduce
			   (Tuple3<String, Double, Integer> cumulative,
				Tuple3<String, Double, Integer> input) throws Exception {
//			System.out.println(" SumAndCount class");
			
			return new Tuple3<String, Double, Integer>(
					input.f0,
					cumulative.f1 + input.f1,
					cumulative.f2 + input.f2);
		}		
	}
	
	public static class Average implements 
			MapFunction<Tuple3<String , Double, Integer>, Tuple2<String, Double>>{

		@Override
		public Tuple2<String, Double> map(Tuple3<String, Double, Integer> input) throws Exception {
			return new Tuple2<String, Double>(
					input.f0,
					input.f1/ input.f2);
		}		
	}

}
