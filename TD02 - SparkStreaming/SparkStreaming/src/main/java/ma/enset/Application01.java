package ma.enset;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/*public class Application01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Sales");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaDStream<String> lines = jssc.textFileStream("ressources/sales.txt.csv");

        JavaDStream<Sale> sales = lines.map(line -> {
            String[] fields = line.split(",");
            return new Sale(fields[0], fields[1], fields[2], Integer.parseInt(fields[3]));
        });

        JavaPairDStream<String, Sale> salesByCity = sales.mapToPair(sale -> new Tuple2<>(sale.getCity(), sale));

        JavaDStream<Long> salesCountByCity = salesByCity.count();

        JavaPairDStream<String, Integer> salesTotalByCity = salesByCity.reduceByKey((sale1, sale2) -> new Sale(sale1.getDate(), sale1.getCity(), sale1.getProduct(), sale1.getPrice() + sale2.getPrice())).mapToPair(sale -> new Tuple2<>(sale._1, sale._2.getPrice()));

        salesCountByCity.print();
        salesTotalByCity.print();

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}*/

/*APPLICATION EN CLASSE*/
/**/
public class Application01 {
    public static void main(String[] args) throws InterruptedException {
        //pour eliminer les messages "logger"
        Logger.getLogger("ma").setLevel(Level.OFF);

        //On doit configurer une duree dans le micro-batch processing
        SparkConf conf = new SparkConf().setAppName("TP1 Spark streaming").setMaster("local[*]");
        JavaStreamingContext sc = new JavaStreamingContext(conf, new Duration(5000));

        //Créer un stream de donnees
        JavaReceiverInputDStream<String> dStreamLines = sc.socketTextStream("localhost",9870);
        JavaDStream<String> dStreamWords = dStreamLines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        // des transformations sur le stream de donnees
        JavaPairDStream<String,Integer> dStreamPaires= dStreamWords.mapToPair(s -> new Tuple2<>(s,1));
        JavaPairDStream<String,Integer> dStreamWordCount = dStreamPaires.reduceByKey((a,b) ->a+b);
        dStreamWordCount.print();
        sc.start();
        sc.awaitTermination();
    }
}
