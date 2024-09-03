package functional;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class GetDataFromFileToTuple implements MapFunction<String, Tuple2<Integer, String>> {

	@Override
	public Tuple2<Integer, String> map(String value) throws Exception {
		String[] tokens = value.split(",");
		return new Tuple2<>(Integer.parseInt(tokens[0]), tokens[1]);
	}

}
