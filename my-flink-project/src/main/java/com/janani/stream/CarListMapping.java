package com.janani.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CarListMapping {

    public static class Car{
        public String make;
        public String model;
        public String type;
        public Float price;

        public Car() {
        }

        public Car(String make, String model, String type, Float price) {
            this.make = make;
            this.model = model;
            this.type = type;
            this.price = price;
        }

        @Override
        public String toString() {
            return "Car{" +
                    "make='" + make + '\'' +
                    ", model='" + model + '\'' +
                    ", type='" + type + '\'' +
                    ", price=" + price +
                    '}';
        }
    }


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSink<Car> dataStream = env.readTextFile("src/main/resources/carList.txt")
                .map(new CreateCarObjects())
//                .filter(new MakePriceFilter("Ford",70000f))
//                applying single map operation using lambda instead if above filter. comment above and uncomment below
                .map(car -> new Car(car.make,car.model,car.type, car.price *1.05f))
                .print();


//        dataStream.filter(new MakePriceFilter("BMW",70000f))
//                .print();

        env.execute("Car List Mapping");

    }
    public static class MakePriceFilter implements FilterFunction<Car> {

        private String make;
        private Float price;

        public MakePriceFilter(String make, Float price) {
            this.make = make;
            this.price = price;
        }

        @Override
        public boolean filter(Car car) throws Exception {
//            System.out.println(car);
            return (car.make).equals(make) && (car.price < price);
        }
    }

    public static class CreateCarObjects implements MapFunction<String,Car>{
        @Override
        public Car map(String s) throws Exception {
            String[] tokens = s.split(",");
            if (tokens.length==4) {
                return new Car(tokens[0], tokens[1], tokens[2], Float.parseFloat(tokens[3]));
            }
            return null;
        }
    }
}
