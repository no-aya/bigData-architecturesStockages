package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Exercice02 {
    public static void main(String[] args) {

        Exercice02 exercice02 = new Exercice02();
        //System.out.println("Sommes des ventes par ville : ");
        //exercice02.totalParVille();
        System.out.println("Sommes des ventes par ville en 2001 : ");
        exercice02.totalParVille("2001");
    }
    public void totalParVille(){
        //Spark
        SparkConf conf = new SparkConf();
        conf.setAppName("Exercice 02").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> sourceRDD=sc.textFile("src/main/resources/ventes.txt");

        //Affichage du contenu du fichier source
        sourceRDD.foreach(entry-> System.out.println(entry));

        //Mappe le fichier source en un RDD de tuple (ville, montant)
        JavaPairRDD<String,Double> rdd1 = sourceRDD.mapToPair(entry->{
            String[] split = entry.split(" ");
            return new Tuple2<>(split[1],Double.valueOf(split[3]));
        });

        //Summe les montants par ville
        JavaPairRDD<String,Double> rdd2 = rdd1.reduceByKey((a, b)->a+b);

        //Affichage du résultat
        rdd2.foreach(entry-> System.out.println(entry));

        //End spark
        sc.stop();
    }

    public void totalParVille(String annee){
        SparkConf conf = new SparkConf();
        conf.setAppName("Exercice 02").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> sourceRDD=sc.textFile("src/main/resources/ventes.txt");

        JavaPairRDD<String,Double> rdd1 = sourceRDD.mapToPair(entry->{
            String[] split = entry.split(" ");
            String[] date = split[0].split("/");
            if(date[2].equals(annee))
                return new Tuple2<>(split[1],Double.valueOf(split[3]));
            else
                return new Tuple2<>("",0.0);
        });

        JavaPairRDD<String,Double> rdd2 = rdd1.reduceByKey((a, b)->a+b);

        //Affichage du résultat et suppression des lignes vides
        rdd2.filter(entry->!entry._1.equals("")).foreach(entry-> System.out.println(entry));

    }

}

