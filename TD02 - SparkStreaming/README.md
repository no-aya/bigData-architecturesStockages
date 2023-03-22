# TD 02 : Application de Spark streaming

## Objectifs
Dans cette activité, nous allons convertir le code de l'activité précédente en une application Spark streaming. Nous allons ensuite lancer cette application sur un cluster Spark.

## Partie 1 : Gestion des ventes
Nous allons traiter les données de ventes de la même manière que dans l'activité précédente. Nous allons donc commencer par créer un projet Maven et ajouter les dépendances suivantes :

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.11</artifactId>
        <version>2.4.0</version>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-streaming_2.11</artifactId>
        <version>2.4.0</version>
    </dependency>
</dependencies> 
```

Le fichier de données de ventes est disponible dans le répertoire ressources de ce projet. Il s'appel `sales.csv`.

La structure de ce fichier est la suivante :

```csv
date,city,product,price
2019-02-10,Marrakech,TV,1000
2019-02-10,Lyon,Phone,500 
2019-02-10,Marseille,TV,1000
2019-02-10,Marrakech,Phone,500
2019-02-10,Marrakech,TV,1000
2019-02-10,Lyon,TV,1000
2019-02-10,Marseille,Phone,500
2019-02-10,Marrakech,Phone,500 
```
On commence par créer la classe `Sale` qui va représenter une vente :

```java
public class Sale {
    private String date;
    private String city;
    private String product;
    private int price;

    public Sale(String date, String city, String product, int price) {
        this.date = date;
        this.city = city;
        this.product = product;
        this.price = price;
    }
}
```
Nous allons créer la classe `Application` qui va contenir le code de notre application. Nous allons commencer par créer un contexte Spark streaming qui va lire les données du fichier `sales.txt` :

```java
public class Application {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Sales");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaDStream<String> lines = jssc.textFileStream("ressources/sales.csv");
    }
}
```

Nous allons ensuite créer un DStream qui va contenir les données de ventes. Nous allons utiliser la méthode `map` pour transformer chaque ligne en objet `Sale` :

```java
JavaDStream<Sale> sales = lines.map(line -> {
    String[] fields = line.split(" ");
    return new Sale(fields[0], fields[1], fields[2], Integer.parseInt(fields[3]));
});
```

Nous allons ensuite créer un DStream qui va contenir les ventes par ville. Nous allons utiliser la méthode `mapToPair` pour transformer chaque vente en paire (ville, vente) :

```java
JavaPairDStream<String, Sale> salesByCity = sales.mapToPair(sale -> new Tuple2<>(sale.getCity(), sale));
```

Nous allons ensuite créer un DStream qui va contenir le nombre de ventes par ville. Nous allons utiliser la méthode `count` pour compter le nombre de ventes par ville :

```java
JavaPairDStream<String, Long> salesCountByCity = salesByCity.count();
```

Nous allons ensuite créer un DStream qui va contenir le montant total des ventes par ville. Nous allons utiliser la méthode `reduce` pour calculer le montant total des ventes par ville :

```java
JavaPairDStream<String, Integer> salesTotalByCity = salesByCity.reduceByKey((sale1, sale2) -> {
    return new Sale(sale1.getDate(), sale1.getCity(), sale1.getProduct(), sale1.getPrice() + sale2.getPrice());
}).mapToPair(sale -> new Tuple2<>(sale._1, sale._2.getPrice()));
```


Nous allons maintenant lancer l'application sur un cluster Spark. Pour cela, nous allons utiliser le script `run.sh` qui est disponible dans le répertoire `scripts` de ce projet. Ce script va lancer un cluster Spark local avec 2 workers. Il va ensuite lancer l'application sur ce cluster.

Le script `run.sh` est le suivant :

```bash
#!/bin/bash

# Start Spark cluster
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-slave.sh spark://localhost:7077

# Run application
mvn clean package
$SPARK_HOME/bin/spark-submit --class "Application" --master spark://localhost:7077 target/td-02-1.0-SNAPSHOT.jar

# Stop Spark cluster
$SPARK_HOME/sbin/stop-slave.sh
$SPARK_HOME/sbin/stop-master.sh
```

Nous allons maintenant lancer l'application en exécutant le script `run.sh` :

```bash
$ ./run.sh
```

Le résultat de l'application est le suivant :

```bash
...
-------------------------------------------
Time: 1550000000000 ms
-------------------------------------------

-------------------------------------------
Time: 1550000001000 ms
-------------------------------------------
(Marrakech,1)
(Marseille,1)
(Lyon,1)

-------------------------------------------
Time: 1550000002000 ms
-------------------------------------------
(Marrakech,2)
(Marseille,1)
(Lyon,1)
  
-------------------------------------------
Time: 1550000003000 ms
-------------------------------------------
(Marrakech,3)
(Marseille,1)
(Lyon,1)
...
```

## Partie 2 : La météo
Nous allons maintenant traiter les données de météo de la même manière que dans l'activité précédente. Nous allons donc commencer par créer un projet Maven et ajouter les dépendances suivantes :

### Rappel de l'activité précédente
Nous avons utilisé les données météo de la ville de Marrakech pour calculer des statistiques sur les températures minimales et maximales. Le jeu de données est mis à la disposition du public par le NCEI.

Il est disponible à l'adresse suivante : https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/2023.csv.gz

Dans cette partie nous allons traiter les données de météo avec Spark Streaming.

### Applicatio02.java
Nous allons créer la classe `Application02` qui va contenir le code de notre application. Nous allons commencer par créer un contexte Spark streaming qui va lire les données du fichier `2023.csv` :

```java
public class Application02 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Weather");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaDStream<String> lines = jssc.textFileStream("ressources/2023.csv");
    }
}
```

La class `Weather` est la suivante :

```java
@Getter @Setter @AllArgsConstructor
public class Weather {
    private String city;
    private String product;
    private String date;
    private int temperatureMin;
    private int temperatureMax;
    private int precipitation;
}
```

Nous allons ensuite créer un DStream qui va contenir les données de météo. Nous allons utiliser la méthode `map` pour transformer chaque ligne en objet `Weather` :
```java
JavaDStream<Weather> weathers = lines.map(line -> {
    String[] fields = line.split(",");
    return new Weather(fields[0], fields[1], fields[2], Integer.parseInt(fields[3]), Integer.parseInt(fields[4]), Integer.parseInt(fields[5]));
});
```

Nous allons ensuite créer un DStream qui va contenir les données de météo de la ville de Marrakech. Nous allons utiliser la méthode `filter` pour ne garder que les données de la ville de Marrakech :

```java
JavaDStream<Weather> MarrakechWeathers = weathers.filter(weather -> weather.getCity().equals("MA000007590"));
```

Nous allons ensuite créer un DStream qui va contenir les températures minimales de la ville de Marrakech. Nous allons utiliser la méthode `map` pour transformer chaque objet `Weather` en température minimale :

```java
JavaDStream<Integer> minMarocTemperatures = marocWeathers.map(weather -> weather.getMinTemperature());
```

Nous allons ensuite créer un DStream qui va contenir les températures maximales de la ville de Marrakech. Nous allons utiliser la méthode `map` pour transformer chaque objet `Weather` en température maximale :

```java
JavaDStream<Integer> maxMarocTemperatures = marrakechWeathers.map(weather -> weather.getMaxTemperature());
```

Code complet de la classe `Application02` :

```java
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

        JavaDStream<Weather> MarrakechWeathers = weathers.filter(weather -> weather.getCity().equals("FR000007590"));

        JavaDStream<Integer> minMarrakechTemperatures = MarrakechWeathers.map(weather -> weather.getMinTemperature());
        JavaDStream<Integer> maxMarrakechTemperatures = MarrakechWeathers.map(weather -> weather.getMaxTemperature());

        jssc.start();
        jssc.awaitTermination();
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
}
```



