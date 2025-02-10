package git.snippets.mr.customorder;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<Order, Text, Order, NullWritable> {
    @Override
    protected void reduce(Order key, Iterable<Text> values, Reducer<Order, Text, Order, NullWritable>.Context context) throws IOException, InterruptedException {
        for (Text text : values) {
            context.write(key,NullWritable.get());
        }
    }
}
