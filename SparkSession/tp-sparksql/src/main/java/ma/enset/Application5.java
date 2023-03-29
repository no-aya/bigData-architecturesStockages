package ma.enset;

import ma.enset.entities.Employee;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;

import static org.apache.log4j.Logger.getLogger;

public class Application5 {
    public static void main(String[] args) {
        getLogger("org.apache").setLevel(Level.OFF);
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("SparkByExamples.com").getOrCreate();
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        Dataset<Employee> dfEmpl = sparkSession.read().json("src/main/resources/employees.json").as(employeeEncoder);

        //D dataframe vers dataset
        Dataset<Employee> dsEmpl = dfEmpl.as(employeeEncoder);
        dsEmpl.show();
        //ou bien en utilisant map
        /*Dataset<Employee> dsEmpl2 = dfEmpl.map(
                (MapFunction<Row, Employee>) row -> new Employee(row.getLong(0), row.getString(1), row.getLong(2), row.getString(3), row.getDouble(4)),
                employeeEncoder
        );*/


        //D dataset vers RDD
        JavaRDD<Employee> rddEmpl = dsEmpl.as(Encoders.bean(Employee.class)).toJavaRDD();
        rddEmpl.foreach(employee -> System.out.println(employee.getName()));

        //D dataframe vers RDD
        JavaRDD<Employee> rddEmpl2 = dfEmpl.as(employeeEncoder).javaRDD();
        rddEmpl2.foreach(employee -> System.out.println(employee.getName()));

        //D RDD vers dataframe
        Dataset<Row> dfEmpl2 = sparkSession.createDataFrame(rddEmpl, Employee.class);
        dfEmpl2.show();

        //D RDD vers dataset
        //Convert javaRDD to RDD
        Dataset<Employee> dsEmpl2 = sparkSession.createDataset(rddEmpl.rdd(), employeeEncoder);
        dsEmpl2.show();

        //DataSet vers DataFrame
        System.out.println("DataSet vers DataFrame");
        Dataset<Row> dfEmpl3 = dsEmpl.toDF();
        dfEmpl3.show();





    }
}
