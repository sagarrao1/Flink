package functional;

import org.apache.flink.api.common.functions.FilterFunction;

public class Filter implements FilterFunction<String>{
	@Override
	public boolean filter(String input) throws Exception {			
		try {
			Double.parseDouble(input.trim());
			return true;
		} catch (Exception e) {
			// TODO: handle exception
		}			
		return false;
	}		
}