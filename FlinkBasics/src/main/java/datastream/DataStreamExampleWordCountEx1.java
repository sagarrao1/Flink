package datastream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import functional.GetDataFromFileAndConvertToMap;

public class DataStreamExampleWordCountEx1 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

//        DataStream<String> text = env.socketTextStream("localhost", 9999); stream of data. nc not installed so reading from file
        DataStream<String> text =  env.readTextFile(params.get("input"));


        SingleOutputStreamOperator<String> filtered = text.filter((FilterFunction<String>) s -> s.startsWith("K"));
        SingleOutputStreamOperator<Tuple2<String,Integer>> map = filtered.map(new GetDataFromFileAndConvertToMap());
        SingleOutputStreamOperator<Tuple2<String, Integer>> counts = map.keyBy(0).sum(1);

        if(params.has("output")){
//            filtered.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        	counts.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
            env.execute("Data Stream API Example");
        }

    }




}
