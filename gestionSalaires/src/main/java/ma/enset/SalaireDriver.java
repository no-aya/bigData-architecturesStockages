package ma.enset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SalaireDriver {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            System.out.println(args[0]);
            System.out.println(args[1]);
            Job job = Job.getInstance(configuration, "Salaires");

            //Les classes Mapper et Reducer
            job.setMapperClass(SalaireMapper.class);

            job.setReducerClass(SalaireReducer.class);

            //Les types de sortie du Mapper et du reducer
            job.setMapOutputKeyClass(Text.class);


            job.setMapOutputValueClass(FloatWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);

            //Le format d'entrée
            job.setInputFormatClass(TextInputFormat.class);

            //le path des Fichiers d'entrées
            TextInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));


            System.out.println(job.waitForCompletion(true) ? 0 : 1);



        } catch (IOException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }

    }
}
