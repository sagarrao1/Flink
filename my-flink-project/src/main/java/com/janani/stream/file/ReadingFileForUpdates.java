package com.janani.stream.file;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

public class ReadingFileForUpdates {
    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        String path ="src/main/resources/riaOlympics.txt";
//        DataStream<String> dataStream = env.readTextFile(path);
//        dataStream.print();
//        env.execute();

        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT,8082);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.
                createLocalEnvironment(1/*,conf*/);
        String path ="src/main/resources/riaOlympics.txt";
        TextInputFormat format = new TextInputFormat(new Path(path));
        format.setFilesFilter(FilePathFilter.createDefaultFilter());

        DataStream<String> inputStream =
                env.readFile(format, path, FileProcessingMode.PROCESS_CONTINUOUSLY, 100);

        inputStream.print();
        env.execute("Processing file continuously");
    }
}
