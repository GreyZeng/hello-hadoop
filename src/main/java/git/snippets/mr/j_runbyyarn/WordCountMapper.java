package git.snippets.mr.j_runbyyarn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static org.apache.commons.lang3.StringUtils.SPACE;

/**
 * Mapper
 * 自定义类继承Mapper抽象类，实现map方法
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text returnKey = new Text();
    IntWritable returnValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(SPACE);
        for (String word : words) {
            returnKey.set(word);
            // 在不去重的基础上，每个单词的数量都记录为1，待mapper阶段再合并
            context.write(returnKey, returnValue);
        }
    }
}
