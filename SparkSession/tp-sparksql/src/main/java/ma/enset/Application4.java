package ma.enset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import ma.enset.entities.Employee;


public class Application4 {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.OFF);
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("SparkByExamples.com").getOrCreate();
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        Dataset<Employee> dsEmpl = sparkSession.read().json("src/main/resources/employees.json").as(employeeEncoder);
        dsEmpl.printSchema();
        dsEmpl.show();

        dsEmpl.filter((FilterFunction<Employee>) employee -> employee.getSalary() > 25000 && employee.getName().startsWith("J")).show();

    }
}
