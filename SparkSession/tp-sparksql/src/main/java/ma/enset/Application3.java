package ma.enset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;


public class Application3 {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("SparkByExamples.com").getOrCreate();
        /*Dataset<Row> dfEmpl = sparkSession.read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/employees")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", "employees")
                .option("user", "root")
                .option("password", "")
                .load();*/
        Map<String, String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://localhost:3306/employees");
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("dbtable", "employees");
        options.put("user", "root");
        options.put("password", "");
        Dataset<Row> dfEmpl = sparkSession.read().format("jdbc").options(options).option("dbtable", "employees").load();
        dfEmpl.printSchema();
        dfEmpl.show();
    }
}
