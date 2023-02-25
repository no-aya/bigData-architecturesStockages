package ma.enset;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static java.lang.Float.parseFloat;

public class VentesMapper extends Mapper<LongWritable, Text, Text, FloatWritable > {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        System.out.println(words[1]);
        String ville = words[1];
        System.out.println(words[2]);
        Float prix = parseFloat(words[2]);
        context.write(new Text(ville), new FloatWritable(prix));


    }
}
