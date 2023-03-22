package ma.enset;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class Application02 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Weather");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaDStream<String> lines = jssc.textFileStream("ressources/2023.csv");

        JavaDStream<Weather> weathers = lines.map(line -> {
            String[] fields = line.split(",");
            return new Weather(fields[0], fields[1], fields[2], Integer.parseInt(fields[3]), Integer.parseInt(fields[4]), Integer.parseInt(fields[5]));
        });

        // le code de marrakech est MA000007590
        JavaDStream<Weather> marrakechWeathers = weathers.filter(weather -> weather.getCity().equals("MA000007590"));

        JavaDStream<Integer> minmarrakechTemperatures = marrakechWeathers.map(weather -> weather.getMinTemperature());
        JavaDStream<Integer> maxmarrakechTemperatures = marrakechWeathers.map(weather -> weather.getMaxTemperature());

        minmarrakechTemperatures.print();
        maxmarrakechTemperatures.print();

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

@Getter @Setter @AllArgsConstructor
class Weather {
    private String city;
    private String product;
    private String date;
    private int temperatureMin;
    private int temperatureMax;
    private int precipitation;

    // Returns the minimum temperature of the day
    public Integer getMinTemperature() {
        return temperatureMin;
    }

    public Integer getMaxTemperature() {
        return temperatureMax;
    }
}