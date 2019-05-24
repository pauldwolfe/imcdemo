package imc.job;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;

import static org.apache.spark.sql.functions.*;

public class AverageClose {

    private final SQLContext spark;

    private AverageClose(final SQLContext spark) {
        this.spark = spark;
    }

    private void run() {
        System.out.println("Starting job");
        spark.read().csv("gs://aapl-quotes").agg(avg(col("_c1")).as("average_close")).write().csv("gs://aapl-quotes/average_close.csv");
        System.out.println("Finishing job");
    }

    public static void main(String[] args) {
        AverageClose averageClose = new AverageClose(SQLContext.getOrCreate(SparkContext.getOrCreate()));
        averageClose.run();
    }
}
