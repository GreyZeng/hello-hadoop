package git.snippets.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * MapReduce任务的驱动类
 * 可以配置任务执行的参数
 * Mapper、Redcuer、分区、分组相关信息
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.创建配置及job对象
        // 由于resource目录下有配置文件了，这里并不是load resource下的配置文件，所以要把参数设置为false
        Configuration conf = new Configuration(false);
        Job job = Job.getInstance(conf);

        //2.设置Driver驱动类
        job.setJarByClass(WordCountDriver.class);

        //3.设置Mapper、Reducer对应的类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4.设置Mapper输出Key,Value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终输出数据的key,value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.设置数据输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("./data/data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./data/output"));

        //7.运行任务
        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.println("任务执行成功！");
        } else {
            System.out.println("任务执行失败！");
        }


    }
}
