package functional;
import org.apache.flink.api.common.functions.FilterFunction;

public class FilterAllNamesStartingWithK implements FilterFunction<String> {

    @Override
    public boolean filter(String s) throws Exception {
        return s.startsWith("K");

    }
}
