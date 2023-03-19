import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("TD 1 Spark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Float> rdd = sc.parallelize(Arrays.asList(1.0f, 2.0f, 23.0f, 14.0f, 5.0f, 6.0f, 70.0f, 8.0f, 9.0f, 10.0f));

        JavaRDD<Float> rdd2 = rdd.map((x) -> x+1);
        JavaRDD<Float> rdd3 = rdd2.filter((x) -> {
            if(x>10) return true;
            else return false;
        });//Filter les nombres pairs
        List<Float> notes = rdd3.collect();
        for(Float note : notes){
            System.out.println(note);
        }

    }
}