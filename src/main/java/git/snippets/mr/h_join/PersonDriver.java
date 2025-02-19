package git.snippets.mr.h_join;

import git.snippets.mr.LocalConfigJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PersonDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.创建配置及job对象
        Job job = LocalConfigJob.getLocalJob();

        //2.设置Driver驱动类
        job.setJarByClass(PersonDriver.class);

        //3.设置Mapper、Reducer对应的类
        MultipleInputs.addInputPath(job, new Path("./data/persondata.txt"), TextInputFormat.class, PersonMapper.class);
        MultipleInputs.addInputPath(job, new Path("./data/addressdata.txt"), TextInputFormat.class, AddressMapper.class);

        job.setReducerClass(PersonReducer.class);

        //4.设置Mapper输出Key,Value类型
        job.setMapOutputKeyClass(PersonInfo.class);
        job.setMapOutputValueClass(PersonInfo.class);

        //5.设置最终输出数据的key,value类型
        job.setOutputKeyClass(PersonInfo.class);
        job.setOutputValueClass(NullWritable.class);

        //6.设置数据输入和输出路径
        FileOutputFormat.setOutputPath(job, new Path("./tmp/join/" + System.currentTimeMillis() + "/output"));

        //7.运行任务
        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.println("任务执行成功！");
        } else {
            System.out.println("任务执行失败！");
        }
    }
}
