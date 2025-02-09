package git.snippets.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper
 * 自定义类继承Mapper抽象类，实现map方法
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Text returnKey = new Text();
    IntWritable returnValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //hello zhangsan
        String line = value.toString();

        //切分单词
        String[] words = line.split(" ");

        //写出数据
        for (String word : words) {
            returnKey.set(word);
            //k,v : word,1
            context.write(returnKey,returnValue);
        }


    }
}
