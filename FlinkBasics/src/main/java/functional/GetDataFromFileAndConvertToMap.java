package functional;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class GetDataFromFileAndConvertToMap implements MapFunction<String, Tuple2<String,Integer>> {

    @Override
    public Tuple2<String, Integer> map(String value) throws Exception {
        return new Tuple2<>(value,1);
    }
}
