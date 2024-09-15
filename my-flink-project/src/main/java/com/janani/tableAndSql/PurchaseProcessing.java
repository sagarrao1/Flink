package com.janani.tableAndSql;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class PurchaseProcessing {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Tuple4<String, String, Integer, String>> inputStream = env.fromElements(
                Tuple4.of("Wiiliam", "TV", 1500, "Amazon"),
                Tuple4.of("Wiiliam", "ipad", 499, "Walmart"),
                Tuple4.of("John", "fitbit", 359, "Amazon"),
                Tuple4.of("Tom", "samsung galaxy", 556, "Target"),
                Tuple4.of("Tom", "TV", 1500, "Amazon"),
                Tuple4.of("Tom", "Headphones", 89, "Amazon"),
                Tuple4.of("kevin", "Airpods", 53, "Apple store")
        );

        tableEnv.createTemporaryView("Purchase",inputStream,
            $("Name"),$("Product"),$("Price"),$("Store"));

        Table tableDetails = tableEnv.from("Purchase");
        Table pDetails = tableDetails.select($("*"));
        TableResult result = pDetails.execute();

//        TableResult result = pDetails.filter($("Store").isEqual("Walmart"))
//                        .execute();

//        TableResult result = tableDetails
//                        .groupBy($("Name"))
//                        .select($("Price").sum().as("Total Spent"), $("Name") )
//                        .execute();

        result.print();
    }
}
