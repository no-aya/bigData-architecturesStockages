package org.example;


import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Struct;
import java.util.concurrent.TimeoutException;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Application03 {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        Logger.getLogger("").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
                .master("local[*]")
                .getOrCreate();

        /*spark.readStream().format("csv").option("header", "true")
                .option("inferSchema", "true")
                .load("src/main/resources/ventes.csv")
                .show();
*/
        StructType schema = new StructType(new StructField[]{
                new StructField("date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ville", DataTypes.StringType, true, Metadata.empty()),
                new StructField("produit", DataTypes.StringType, true, Metadata.empty()),
                new StructField("prix", DataTypes.FloatType, true, Metadata.empty())
        });

        Dataset<Row> df = spark.readStream()
                .format("csv")
                .option("header", "true")
                .option("sep", ",")
                .schema(schema)
                .load("http://localhost:9000/input/");
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
