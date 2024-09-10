package com.janani.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// imaging we are getting this data from sensor placed in highway
public class HighSpeedVehicleDetection {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = 
				StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<String> dataStream = env.
				socketTextStream("localhost", 9000)
				.filter(new Filter());
		
		dataStream.print();
		env.execute("Car speed detection");
	}
	
	public static class Filter implements FilterFunction<String> {

		@Override
		public boolean filter(String input) throws Exception {
			String[] tokens = input.split(",");
//			assume 1st toekn is name of the car and 2nd token is speed of the car
			
			
			
			return false;
		}
		
	}

}
