package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;

public class Test {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Integer> integerDataSource = executionEnvironment.fromElements(1, 2, 3, 34);
        FilterOperator<Integer> filter = integerDataSource.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer.equals(2);
            }
        });
        filter.collect();
        executionEnvironment.execute();
    }
}
