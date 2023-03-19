package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Exercice01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Exercice 01").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("aya","aya2","aya3","aya4","aya5","aya6","aya7","aya8","aya9","aya10","aya11","aya12","aya13","aya14","aya15","aya16","aya17","aya18","aya19","aya20","aya21","aya22","aya23","aya24","aya25","aya26","aya27","aya28","aya29","aya30","aya31","aya32","aya33","aya34","aya35","aya36","aya37","aya38","aya39","aya40","aya41","aya42","aya43","aya44","aya45","aya46","aya47","aya48","aya49","aya50","aya51","aya52","aya53","aya54","aya55","aya56","aya57","aya58","aya59","aya60","aya61","aya62","aya63","aya64","aya65","aya66","aya67","aya68","aya69","aya70","aya71","aya72","aya73","aya74","aya75","aya76","aya77","aya78","aya79","aya80","aya81","aya82","aya83","aya84","aya85","aya86","aya87","aya88","aya89","aya90","aya91","aya92","aya93","aya94","aya95","aya96","aya97","aya98","aya99","aya100");

        // Parallelize the list to create a RDD
        JavaRDD<String> rdd1 = sc.parallelize(list);

        // Flatmap
        JavaRDD<String>  rdd2= rdd1.flatMap(s-> Arrays.asList(s).iterator());
        System.out.println("*********** RDD2 *********** ");
        rdd2.foreach(name-> System.out.print(name+"|"));

        // Filter
        JavaRDD<String>  rdd3= rdd2.filter(s-> s.contains("aya"));
        System.out.println("*********** RDD3/4/5 ***********");
        System.out.print("Names that contains 'aya' : ");
        rdd3.foreach(name-> System.out.println(name + " | "));

        JavaRDD<String>  rdd4= rdd3.filter(s-> s.contains("1"));
        System.out.println("Names that contains '1' : ");
        rdd4.foreach(name-> System.out.println(name + " | "));

        JavaRDD<String>  rdd5= rdd3.filter(s-> s.contains("2"));
        System.out.println("Names that contains '2' : ");
        rdd5.foreach(name-> System.out.println(name + " | "));

        // Union
        JavaRDD<String> rdd6= rdd3.union(rdd4);
        System.out.println("*********** RDD6 ***********");
        rdd6.foreach(name-> System.out.println(name + " | "));

        // Map
        JavaRDD<String> rdd71= rdd5.map(s-> s.toUpperCase());
        System.out.println("*********** RDD71 ***********");
        rdd71.foreach(name-> System.out.println(name + " | "));

        JavaRDD<String> rdd81= rdd6.map(s-> s.toLowerCase());
        System.out.println("*********** RDD81 ***********");
        rdd81.foreach(name-> System.out.println(name + " | "));

        // ReduceByKey
        JavaPairRDD<String,Integer> rdd7 = rdd71.mapToPair((name)->new Tuple2<>(name,1));
        System.out.println("*********** RDD7 ***********");
        rdd7.foreach((name)-> System.out.println(name));

        JavaPairRDD<String,Integer> rdd8 = rdd81.mapToPair((name)->new Tuple2<>(name,1));
        System.out.println("*********** RDD8 ***********");
        rdd8.foreach((name)-> System.out.println(name));

        //Union
        JavaPairRDD<String,Integer> rdd9 = rdd7.union(rdd8);
        System.out.println("*********** RDD9 ***********");
        rdd9.foreach((name)-> System.out.println(name));

        // SortBy
        JavaPairRDD<String,Integer> rdd10 = rdd9.sortByKey();
        System.out.println("*********** RDD10 ***********");
        rdd10.foreach((name)-> System.out.println(name));

    }
}
