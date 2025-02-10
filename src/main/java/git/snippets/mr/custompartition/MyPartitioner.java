package git.snippets.mr.custompartition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器
 */
public class MyPartitioner extends Partitioner<Text, IntWritable> {


    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        String key = text.toString();
        if("zhangsan".equals(key)||"lisi".equals(key)){
            return 0;
        }else if("wangwu".equals(key)){
            return 1;

        }else{
            return 2;
        }
    }
}
