package com.janani.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// getting data from file
public class LifeExpectancyTracking {

	public static void main(String[] args) throws Exception {
//		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env
				= StreamExecutionEnvironment.getExecutionEnvironment();
//		env.getConfig().setGlobalJobParameters(params);
//		DataStream<String> dataStream = StreamUtil.getDataStream(env, params);
//		if (dataStream == null) {
//			System.exit(1);
//			return;
//		}

//		adding code to read directly from main/resource/file.
		DataStream<String> dataStream = env.readTextFile("src/main/resources/Life_Expectancy_Data.csv");


		DataStream<String> lifeStream = dataStream
				.filter(new Filter());

		lifeStream.print();
		env.execute("Life Expectancy Tracking");
	}
	
	public static class Filter implements FilterFunction<String> {
		@Override
		public boolean filter(String input)  {
			if (input.contains("Country,Year,Status,Life expectancy ,Adult Mortality,infant deaths,Alcohol,percentage expenditure,Hepatitis B,Measles , BMI ,under-five deaths ,Polio,Total expenditure,Diphtheria , HIV/AIDS,GDP,Population, thinness  1-19 years, thinness 5-9 years,Income composition of resources,Schooling")){
				return false;
			}

			String[] tokens = new String[0];
			try {
				tokens = input.split(",");
			} catch (Exception e) {
				System.out.println("Null:"+tokens[0]+tokens[1]+tokens[2]+tokens[3]);
				e.printStackTrace();
			}

//			try {
//				Thread.sleep(10000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//			if (tokens[3].isEmpty()){
//				System.out.println("Null:"+tokens[0]+tokens[1]+tokens[2]+tokens[3]+tokens[4]);
//			}
//			added this as we have null values !tokens[3].isEmpty()
			if( !tokens[3].isEmpty() && Double.parseDouble(tokens[3].trim()) > 75 ) {
				return true;
			}
			return false;
		}
		
	}

}
