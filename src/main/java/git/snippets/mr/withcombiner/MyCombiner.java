package git.snippets.mr.withcombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyCombiner extends Reducer<Text, IntWritable,Text, IntWritable> {
    IntWritable total = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        int sum = 0;
        //累加
        for (IntWritable value : values) {

            sum += value.get();
        }
        //设置当前key 对应的value值，就是当前单词对应的 次数
        total.set(sum);

        //结果输出
        context.write(key,total);
        
    }
}
