package org.myorg.quickstart;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;

public class Test {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tup> tupDataSource = executionEnvironment.fromElements(new Tup(1, 10), new Tup(1, 10), new Tup(2, 20), new Tup(2, 20));
        AggregateOperator<Tup> sum = tupDataSource.sum(0);
        sum.print();
        executionEnvironment.execute("hahah");
    }

}
class Tup{
    private int id;
    private int score;

    public Tup(int id, int score) {
        this.id = id;
        this.score = score;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }
}
