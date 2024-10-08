package functional;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class TokenizerForMap implements MapFunction<String, Tuple2<String,Integer>> {

    @Override
    public Tuple2<String, Integer> map(String s) throws Exception {
        return new Tuple2<>(s,1);

    }
}
