package git.snippets.mr.c_custompartition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器案例
 */
public class MyPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
		String key = text.toString();
		if ("test".equals(key) || "WordCount".equals(key)) {
			return 0;
		} else if ("Hadoop".equals(key)) {
			return 1;
		} else {
			return 2;
		}
	}
}
