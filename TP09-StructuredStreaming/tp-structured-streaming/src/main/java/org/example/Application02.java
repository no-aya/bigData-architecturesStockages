package org.example;


import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Application02 {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        Logger.getLogger("").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example").master("local[*]").getOrCreate();
        Dataset<Row> df = spark.readStream().format("socket").option("host", "localhost").option("port", 8888).load();
        df.printSchema();

        Encoder<Vente> venteEncoder = Encoders.bean(Vente.class);
        Dataset<Vente> ds = df.as(Encoders.STRING())
                .map((MapFunction<String, Vente>) value -> {
                    String[] parts = value.split(",");
                    Vente vente = new Vente();
                    vente.setDate(parts[0]);
                    vente.setVille(parts[1]);
                    vente.setProduit(parts[2]);
                    vente.setPrix(Float.parseFloat(parts[3]));
                    return vente;
                }, venteEncoder);

        //Pour importer les données et puis les stocker pour des manipulations futures
        StreamingQuery query = df.writeStream()
                .format("console")
                .trigger(Trigger.ProcessingTime(5000))
                .outputMode("append")
                .start();


        ds.printSchema();

        //Calculer le nombre de ventes par ville
        Dataset<Row> dfVille = ds.groupBy("ville").count();
        //dfVille.show();



        query.awaitTermination();
    }
}
