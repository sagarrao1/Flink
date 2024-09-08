package jananiravi.stream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import functional.Filter;

// This will Filter string and print only numbers
public class FilterStrings {
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env= 
				StreamExecutionEnvironment.getExecutionEnvironment();
			
		DataStream<String> dataStream = env.
									socketTextStream("localhost",9999)
									.filter(new Filter());
		
		dataStream.print();	
		env.execute("FilterStrings Strings ");		
	}	
	
}
