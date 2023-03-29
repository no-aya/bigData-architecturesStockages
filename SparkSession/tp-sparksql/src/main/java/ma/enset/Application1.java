package ma.enset;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;


public class Application1 {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.OFF);
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("SparkByExamples.com").getOrCreate();
        //Json
        Dataset<Row> dfEmpl = sparkSession.read().option("multiline",true).json("src/main/resources/employees.json");
        dfEmpl.printSchema();
        // Dataframe est un dataset de type Row
        // Dataset est un dataframe avec des types de données

        dfEmpl.select("name","salary").show();

        //Ajouter 2000 aà chaque salaire
        dfEmpl.select(col("name"),col("salary").plus(2000)).show();

        //Filter
        dfEmpl.filter(col("salary").gt(25000).and(col("name").startsWith("J"))).show();
        // Si on veut utiliser une expression SQL
        dfEmpl.filter("salary > 25000 and name like 'J%'").show();

        //Pour exécuter des requêtes sur des tables temporaires
        dfEmpl.createOrReplaceTempView("employees");
        sparkSession.sql("select * from employees where salary > 25000 and name like 'J%'").show();


        /* TODO
         * Application 1
         * 1- Afficher le salaire moyen par département
         * 2- Afficher le salaire min et max par département
         */

        //1- Afficher le salaire moyen par département
        // On a 2 options pour faire ça : sql ou dataframe
        // Avec sql
        sparkSession.sql("select department, avg(salary) as avg_salary from employees group by department").show();
        // Avec dataframe
        dfEmpl.groupBy("department").avg("salary").show();

        //2- Afficher le salaire min et max par département
        // Avec sql
        sparkSession.sql("select department, min(salary) as min_salary, max(salary) as max_salary from employees group by department").show();
        // Avec dataframe
        dfEmpl.groupBy("department").min("salary").show();
        dfEmpl.groupBy("department").max("salary").show();





    }
}
