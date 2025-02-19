package git.snippets.mr.f_customreduceorderwithpartitioner;

import git.snippets.mr.LocalConfigJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 需求：获取每个月最高温和次高温的数据
 */
public class TemperatureDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.创建配置及job对象
        Job job = LocalConfigJob.getLocalJob();

        //2.设置Driver驱动类
        job.setJarByClass(TemperatureDriver.class);

        //3.设置Mapper、Reducer对应的类
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TempertureReducer.class);

        //4.设置Mapper输出Key,Value类型
        job.setMapOutputKeyClass(Temperature.class);
        job.setMapOutputValueClass(Temperature.class);

        //5.设置最终输出数据的key,value类型
        job.setOutputKeyClass(Temperature.class);
        job.setOutputValueClass(NullWritable.class);

        //设置自定义分区器
        job.setPartitionerClass(TemperaturePartitioner.class);

        //设置ReduceTask 个数
        job.setNumReduceTasks(3);

        //6.设置数据输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("./data/temperatureData.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./tmp/customreduceorderwithpartitioner/" + System.currentTimeMillis() + "/output"));

        //7.运行任务
        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.println("任务执行成功！");
        } else {
            System.out.println("任务执行失败！");
        }
    }
}
