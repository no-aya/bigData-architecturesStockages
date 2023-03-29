package org.example;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("SparkByExamples.com").getOrCreate();
        //Json
        Dataset<Row> dfEmpl = sparkSession.read().option("multiline",true).json("src/main/resources/employees.json");
        dfEmpl.printSchema();
        // Dataframe est un dataset de type Row
        // Dataset est un dataframe avec des types de données

        dfEmpl.select("name","salary").show();

    }
}
