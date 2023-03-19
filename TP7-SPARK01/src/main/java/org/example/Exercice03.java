package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

public class Exercice03 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Exercice03").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> meteo= sc.textFile("src/main/resources/2023.csv");
        JavaPairRDD<String, Double> Temp = meteo.mapToPair(ligne -> new Tuple2<>(ligne.split(",")[2], Double.parseDouble(ligne.split(",")[3])));


        //Temperature minimale moyenne
        JavaPairRDD<String,Double> Tmin = Temp.filter(x -> x._1.equals("TMIN"));
        Long taille = Tmin.count();
        JavaPairRDD<String, Double> Tmin_som = Tmin.reduceByKey((x, y) -> x + y);
        JavaPairRDD<String, Double> Tmin_moy = Tmin_som.mapValues(x -> x/taille);
        System.out.println(Tmin_moy.collect());

        //Temperature maximale moyenne
        JavaPairRDD<String,Double> Tmax = Temp.filter(x -> x._1.equals("TMAX"));
        Long taille1 = Tmax.count();
        JavaPairRDD<String, Double> Tmax_som = Tmax.reduceByKey((x, y) -> x + y);
        JavaPairRDD<String, Double> Tmax_moy = Tmax_som.mapValues(x -> x/taille1);
        System.out.println(Tmax_moy.collect());

        //Tmperature max la plus elevee
        JavaPairRDD<String, Double> Tmax2 = Tmax.reduceByKey((x, y) -> Math.max(x,y));
        System.out.println(Tmax2.collect());

        //Temperature min la plus basse
        JavaPairRDD<String, Double> Tmin2 = Tmin.reduceByKey((x, y) -> Math.min(x,y));
        System.out.println(Tmin2.collect());

        //New list with ids and temperatures
        //3 colonnes : id, type de temperature, valeur de la temperature
        JavaRDD<Tuple3<String,String,Double>> stations = meteo.map(ligne -> new Tuple3<>(ligne.split(",")[0],ligne.split(",")[2],Double.parseDouble(ligne.split(",")[3])));

        //Top 5 stations les plus chaudes : prendre la liste des stations et temparatures, prendre les stations avec TMAX, prendre la valeur max de chaque station*
        JavaPairRDD<String, Double> Tmax3 = stations.filter(x -> x._2().equals("TMAX")).mapToPair(x -> new Tuple2<>(x._1(), x._3()));
        JavaPairRDD<String, Double> Tmax4 = Tmax3.reduceByKey((x, y) -> Math.max(x,y));
        JavaPairRDD<Double, String> Tmax5 = Tmax4.mapToPair(x -> new Tuple2<>(x._2, x._1));
        JavaPairRDD<Double, String> Tmax6 = Tmax5.sortByKey(false);
        System.out.println(Tmax6.take(5));

        //Top 5 stations les plus froides : prendre la liste des stations et temparatures, prendre les stations avec TMIN, prendre la valeur min de chaque station*
        JavaPairRDD<String, Double> Tmin3 = stations.filter(x -> x._2().equals("TMIN")).mapToPair(x -> new Tuple2<>(x._1(), x._3()));
        JavaPairRDD<String, Double> Tmin4 = Tmin3.reduceByKey((x, y) -> Math.min(x,y));
        JavaPairRDD<Double, String> Tmin5 = Tmin4.mapToPair(x -> new Tuple2<>(x._2, x._1));
        JavaPairRDD<Double, String> Tmin6 = Tmin5.sortByKey(true);
        System.out.println(Tmin6.take(5));



    }
}
