package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;
import scala.Tuple2;

public class Test {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> stringDataSource = executionEnvironment.readTextFile("/word.txt");
        AggregateOperator<Tuple2<String, Integer>> sum = stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split("\\W+");
                for (String s1 : split) {
                    if (s1.length() > 0) {
                        collector.collect(new Tuple2<String, Integer>(s1, 1));
                    }
                }
            }
        }).groupBy(0).sum(1);
        sum.writeAsCsv("/res.txt");
        executionEnvironment.execute("hello wordcount");

    }

}
