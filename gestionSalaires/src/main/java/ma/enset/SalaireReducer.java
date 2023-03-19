package ma.enset;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SalaireReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    /**
     * dep1-min XXXX.XX
     * dep1-max XXXX.XX
     * dep2-min XXXX.XX
     * dep2-max XXXX.XX
     */
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        for (FloatWritable value : values) {
            float salaire = value.get();
            System.out.println(salaire);
            if (salaire < min) min = salaire;
            if (salaire > max) max = salaire;
        }
        context.write(new Text(key.toString() + "-min"), new FloatWritable(min));
        context.write(new Text(key.toString() + "-max"), new FloatWritable(max));

    }

}
