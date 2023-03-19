package ma.enset;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static java.lang.Float.parseFloat;

public class SalaireMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("hello");
        String[] values = value.toString().split(",");
        String departement = values[2];
        Float salaire = Float.parseFloat(values[4]);

        context.write(new Text(departement), new FloatWritable(salaire));
    }

}

