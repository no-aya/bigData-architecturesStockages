# TP 3 : MapReduce et YARN
## 1. Mise en place de l'environnement
Dans notre machine virtuelle, nous avons déjà installé Hadoop. Nous allons donc directement passer à la configuration de ces deux outils.
### 1.1. Configuration de Hadoop
#### 1.1.1. Configuration de HDFS
Nous allons commencer par configurer HDFS. Pour cela, nous allons modifier le fichier `core-site.xml` qui se trouve dans le dossier `etc/hadoop` de notre installation Hadoop. Nous allons modifier les lignes suivantes :
```xml
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
</property>
```
Nous allons ensuite modifier le fichier `hdfs-site.xml` qui se trouve dans le dossier `etc/hadoop` de notre installation Hadoop. Nous allons modifier les lignes suivantes :
```xml
<property>
    <name>dfs.replication</name>
    <value>1</value>
</property>
```
#### 1.1.2. Configuration de YARN
Nous allons ensuite configurer YARN. Pour cela, nous allons modifier le fichier `yarn-site.xml` qui se trouve dans le dossier `etc/hadoop` de notre installation Hadoop. Nous allons modifier les lignes suivantes :
```xml
<property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
</property>
```
#### 1.1.3. Configuration de MapReduce
Nous allons ensuite configurer MapReduce. Pour cela, nous allons modifier le fichier `mapred-site.xml` qui se trouve dans le dossier `etc/hadoop` de notre installation Hadoop. Nous allons modifier les lignes suivantes :
```xml
<property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
</property>
```
### 1.2. Configuration de Hadoop
Nous allons maintenant configurer Hadoop. Pour cela, nous allons modifier le fichier `hadoop-env.sh` qui se trouve dans le dossier `etc/hadoop` de notre installation Hadoop. Nous allons modifier la ligne suivante :
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```
### 1.3. Formatage du système de fichiers HDFS
Nous allons maintenant formater le système de fichiers HDFS. Pour cela, nous allons exécuter la commande suivante :
```bash
hdfs namenode -format
```
### 1.4. Démarrage de Hadoop
Nous allons maintenant démarrer Hadoop. Pour cela, nous allons exécuter la commande suivante :
```bash
start-dfs.sh
```
Nous allons ensuite démarrer YARN. Pour cela, nous allons exécuter la commande suivante :
```bash
start-yarn.sh
```
### 1.5. Vérification du bon fonctionnement de Hadoop
Nous allons maintenant vérifier que Hadoop fonctionne correctement. Pour cela, nous allons exécuter la commande suivante :
```bash
jps
```
Nous devrions obtenir le résultat suivant :
```bash
$ jps
1053 Jps
1029 NameNode
1045 DataNode
1035 SecondaryNameNode
```
## 2. MapReduce
### 2.1. Exécution d'un job MapReduce
Nous allons maintenant exécuter un job MapReduce. Pour cela, nous allons créer un fichier `ventes.txt` qui contient les lignes suivantes :
```bash
2022-02-03 paris pc 1500.3
2022-02-03 marrakech laptop 2536.2
2022-01-02 paris laptop 15236.3
2022-02-03 marrakech pc 1520.3
2022-02-21 paris pc 15243.0
```

Nous cherchons à compter le total des ventes par ville. Pour cela, nous allons créer un job MapReduce qui va lire le fichier `ventes.txt` et va compter le nombre de ventes par ville. Pour cela, nous allons créer les fichiers suivants :
- `VentesMapper.java` :
```java
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VentesMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        context.write(new Text(tokens[1]), new LongWritable(1));
    }
}
```
- `VentesReducer.java` :
```java
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VentesReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        context.write(key, new LongWritable(sum));
    }
}
```
- `VentesDriver.java` :
```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VentesDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Ventes");
        job.setJarByClass(VentesDriver.class);
        job.setMapperClass(VentesMapper.class);
        job.setReducerClass(VentesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```
Nous allons maintenant exécuter le job MapReduce. Pour cela, nous allons exécuter la commande suivante :
```bash
hadoop jar /home/hadoop/ventes.jar VentesDriver /user/hadoop/ventes.txt /user/hadoop/ventes-output
```
Nous allons ensuite afficher le résultat du job MapReduce. Pour cela, nous allons exécuter la commande suivante :
```bash
hdfs dfs -cat /user/hadoop/ventes-output/part-r-00000
```
Nous devrions obtenir le résultat suivant :
```bash
$ hdfs dfs -cat /user/hadoop/ventes-output/part-r-00000
marrakech 4056.5
paris 31979.6
```




