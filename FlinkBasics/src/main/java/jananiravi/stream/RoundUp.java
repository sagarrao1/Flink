package jananiravi.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import functional.Filter;

public class RoundUp {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env= 
				StreamExecutionEnvironment.getExecutionEnvironment();
			
		DataStream<Long> dataStream = env.
									socketTextStream("localhost",9999)
									.filter(new Filter())
									.map(new Round());
									
		
		dataStream.print();	
		env.execute("FilterStrings Strings ");	
	}
	
	public static class Round implements MapFunction<String, Long>{
		@Override
		public Long map(String input) throws Exception {
			double d = Double.parseDouble(input);
			return Math.round(d);
		}		
	}

}
