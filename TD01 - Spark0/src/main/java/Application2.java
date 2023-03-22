import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Application2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("TD 1 Spark : word count").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("src/main/resources/words.txt");
        JavaRDD<String> rdd2 = rdd.flatMap((x) -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordMap = rdd2.mapToPair((x) -> new Tuple2<>(x, 1));
        JavaPairRDD<String, Integer> wordCount = wordMap.reduceByKey((x, y) -> x + y);


        //Affichage
        List<Tuple2<String, Integer>> list=wordCount.collect();
        for(Tuple2<String, Integer> tuple : list){
            System.out.println(tuple._1 + " : " + tuple._2);
        }


    }
}
