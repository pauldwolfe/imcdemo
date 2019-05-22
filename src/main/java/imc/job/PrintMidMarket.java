package imc.job;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;

public class PrintMidMarket {

    private final SQLContext spark;

    public PrintMidMarket(final SQLContext spark) {
        this.spark = spark;
    }

    void run() {
        System.out.println("Starting job");
        spark.read().csv("gs://aapl-quotes").collectAsList().forEach(System.out::println);
        System.out.println("Finishing job");
    }

    public static void main(String[] args) {
        PrintMidMarket printMidMarket = new PrintMidMarket(SQLContext.getOrCreate(SparkContext.getOrCreate()));
        printMidMarket.run();
    }
}
