package ma.enset;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VentesReduce extends Reducer<Text, FloatWritable,Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) {
        float sum = 0;
        for (FloatWritable value : values) {
            sum += value.get();
        }
        try {
            context.write(key, new FloatWritable(sum));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
