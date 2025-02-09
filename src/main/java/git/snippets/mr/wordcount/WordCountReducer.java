package git.snippets.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduce端
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {
    IntWritable total = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //记录当前key的value值
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        //设置value
        total.set(sum);

        context.write(key,total);


    }
}
