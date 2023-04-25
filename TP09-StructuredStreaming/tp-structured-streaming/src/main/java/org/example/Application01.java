package org.example;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Application01 {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        Logger.getLogger("").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example").master("local[*]").getOrCreate();
        Dataset<Row> df = spark.readStream().format("socket").option("host", "localhost").option("port", 8888).load();
        df.printSchema();

        //Pour importer les données et puis les stocker pour des manipulations futures
        StreamingQuery query = df.writeStream()
                .format("console")
                .trigger(Trigger.ProcessingTime(5000))
                .outputMode("append")
                .start();

        query.awaitTermination();
    }
}
