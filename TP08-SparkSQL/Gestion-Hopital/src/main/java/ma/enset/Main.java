package ma.enset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.immutable.Seq;
import org.apache.spark.sql.functions;

import java.util.HashMap;
import java.util.Map;


public class Main {
    public static void main(String[] args) {
        Logger.getLogger("").setLevel(Level.OFF);
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("SparkByExamples.com").getOrCreate();

        /*System.out.println("*****Afficher le nombre de consultation par date*****");
        System.out.println("Consultations par date | Méthode DataFrame");
        Dataset<Row> df = sparkSession.read().format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("user", "root")
                .option("password","")
                .option("dbtable","CONSULTATIONS")
                .load();

        df.select("DATE_CONSULTATION").groupBy("DATE_CONSULTATION").count().alias("Nombre de consultations").show();
        //Consultations par date
        df.createOrReplaceTempView("consultationsParDate");
        //Requête SQL


        System.out.println("Consultations par date | Méthode SQL");
        Dataset<Row> dfConsultationsParDate = sparkSession
                .sql("SELECT DATE_CONSULTATION, COUNT(*) AS NombreConsultations FROM consultationsParDate GROUP BY DATE_CONSULTATION");
        dfConsultationsParDate.show();



        System.out.println("*****Afficher le nombre de consultation par médecin*****");
        //df.createOrReplaceTempView("consultationsParMedecin");
        //Requête SQL

        System.out.println("Consultations par médecin | Méthode SQL");
        String sql = "Select medecins.Nom, medecins.Prenom, count(*) as NombreConsultation FROM medecins INNER JOIN consultations ON medecins.ID=consultations.ID_MEDECIN GROUP BY medecins.ID";
        Dataset<Row> df2 = sparkSession.read().format("jdbc").
                option("query",sql)
                .option("url", "jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("user", "root")
                .option("password","").load();
        df2.show();
*/



        System.out.println("*****Afficher le nombre de patients traité par médecin*****");
        /*System.out.println("Patients traités par médecin | Méthode SQL");
        String sql = "SELECT MEDECINS.NOM, MEDECINS.PRENOM, COUNT(DISTINCT PATIENTS.ID) AS NombrePatients FROM consultations INNER JOIN MEDECINS ON consultations.ID_MEDECIN = MEDECINS.ID INNER JOIN PATIENTS ON consultations.ID_PATIENT = PATIENTS.ID GROUP BY MEDECINS.NOM, MEDECINS.PRENOM";

        Dataset<Row> df = sparkSession.read().format("jdbc").
                option("query",sql)
                .option("url", "jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("user", "root")
                .option("password","").load();
        df.show();
*/

        System.out.println("Patients traités par médecin | Méthode DataSet");

        Dataset<Row> consultation=sparkSession.read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("user", "root")
                .option("password","")
                .option("dbtable","CONSULTATIONS")
                .load();

        Dataset<Row> medecin=sparkSession.read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("user", "root")
                .option("password","")
                .option("dbtable","MEDECINS")
                .load();
        Dataset<Row> jointureDF = consultation.join(medecin, consultation.col("id_medecin").equalTo(medecin.col("id")));
        jointureDF.select("ID_MEDECIN","NOM","PRENOM","ID_PATIENT").groupBy("ID_MEDECIN","NOM","PRENOM").agg(org.apache.spark.sql.functions.countDistinct("ID_PATIENT").alias("NombrePatients")).show();





    }
}