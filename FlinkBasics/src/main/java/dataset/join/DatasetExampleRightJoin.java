package dataset.join;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

import functional.GetDataFromFileToTuple;
import functional.RightJoinNameAndLocation;

public class DatasetExampleRightJoin {

public static void main(String[] args) throws Exception {
	 final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);
        
//	        Matching records from both files
        DataSet<String> personFile   = env.readTextFile(params.get("input1"));
        DataSet<String> locationFile = env.readTextFile(params.get("input2"));
        
        MapOperator<String, Tuple2<Integer, String>> personSet = personFile.map(new GetDataFromFileToTuple());
        MapOperator<String, Tuple2<Integer, String>> locationSet = locationFile.map(new GetDataFromFileToTuple());
        
        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> joined 
        		= personSet.rightOuterJoin(locationSet).where(0).equalTo(0).with(new RightJoinNameAndLocation());
        
        joined.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        env.execute("DatasetExampleLeftOuterJoin");
        
	}

}
