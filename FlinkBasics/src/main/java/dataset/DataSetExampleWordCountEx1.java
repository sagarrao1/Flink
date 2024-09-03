package dataset;

import functional.FilterAllNamesStartingWithK;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import functional.TokenizerForMap;

public class DataSetExampleWordCountEx1 {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

//        1. Read file
        DataSet<String> text =  env.readTextFile(params.get("input")); // under env refer other readText methods

//        2. Filter the data
//        DataSet<String> filtered = text.filter((FilterFunction<String>) s -> s.startsWith("K"));
        DataSet<String> filtered = text.filter(new FilterAllNamesStartingWithK()); //output is ["Karthik","Kartic","Karthika"]

//        3. Split names and assign 1 to each for counting later
        DataSet<Tuple2<String,Integer>> tokenized = filtered.map(new TokenizerForMap());

//        4. Grouping all names
        UnsortedGrouping<Tuple2<String, Integer>> names = tokenized.groupBy(0);

//        5. sum to group all names to get number
        AggregateOperator<Tuple2<String, Integer>> counts = names.sum(1);

        if(params.has("output")){
            counts.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
            env.execute("Word count");
        }

    }




}
