package git.snippets.mr.withcombiner;

import git.snippets.mr.LocalConfigJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce任务的驱动类
 * 可以配置任务执行的参数
 * Mapper、Reducer、分区、分组相关信息
 */
public class WordCountDriver {
    // 预先聚合操作，wordcount适合，但是有一些场景不太适合，例如求平均数
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.创建配置及job对象
        Job job = LocalConfigJob.getLocalJob();
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

        //设置map 端的combiner 本质就是WoundCountReducer
        job.setCombinerClass(MyCombiner.class);

        //6.设置数据输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("./data/data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./tmp/output8"));

        //7.运行任务
        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.println("任务执行成功！");
        } else {
            System.out.println("任务执行失败！");
        }


    }
}
