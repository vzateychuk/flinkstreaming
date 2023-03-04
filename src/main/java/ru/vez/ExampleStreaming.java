package ru.vez;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ExampleStreaming {

  public static void main(String[] args) throws Exception {

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      DataStream<Person> flintstones = env.fromElements(
              new Person("Fred", 35),
              new Person("Wilma", 35),
              new Person("Pebbles", 2)
      );

      DataStream<Person> adults = flintstones.filter( person -> person.getAge() >= 18 );

      adults.print();

      env.execute();
  }
}
