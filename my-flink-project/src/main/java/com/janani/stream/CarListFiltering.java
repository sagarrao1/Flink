package com.janani.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CarListFiltering {

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

        DataStream<Car> dataStream = env.fromElements(
                new Car("BMW","2022","sedan",6919f),
                new Car("BMW","2023","sedan",7319f),
                new Car("BMW","2024","sedan",14610f),
                new Car("chervolet","2022","truck",36100f),
                new Car("chervolet","2023","sedan",16400f),
                new Car("chervolet","2022","truck",15210f),
                new Car("Ford","2022","sedan",41575f),
                new Car("Ford","2022","sedan",13730f),
                new Car("Ford","2022","sedan",10730f),
                new Car("Kia","2024","sedan",20140f)
        );

        dataStream.filter(new MakePriceFilter("BMW",70000f))
                .print();
        env.execute("Car List Filtering");

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
            return (car.make).equals(make) && (car.price < price);
        }
    }
}
