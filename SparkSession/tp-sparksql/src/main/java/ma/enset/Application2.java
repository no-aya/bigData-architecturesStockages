package ma.enset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application2 {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.OFF);
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("SparkByExamples.com").getOrCreate();
        //CSV
        Dataset<Row> dfEmpl = sparkSession.read().option("header",true).option("inferSchema",true).csv("src/main/resources/employees.csv");
        dfEmpl.printSchema();
        dfEmpl.show();

    }
}
