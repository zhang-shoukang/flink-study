package org.myorg.quickstart;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;

public class Test {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Integer> integerDataSource = executionEnvironment.fromElements(1, 2, 3, 34);
        AggregateOperator<Integer> sum = integerDataSource.sum(0);
        sum.print();
        executionEnvironment.execute("hahah");
    }
}
