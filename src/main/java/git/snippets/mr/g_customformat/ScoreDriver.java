package git.snippets.mr.g_customformat;

import git.snippets.mr.LocalConfigJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ScoreDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.创建配置及job对象
        //1.创建配置及job对象
        Job job = LocalConfigJob.getLocalJob();

        //2.设置Driver驱动类
        job.setJarByClass(ScoreDriver.class);

        //3.设置Mapper、Reducer对应的类
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReducer.class);

        //4.设置Mapper输出Key,Value类型
        job.setMapOutputKeyClass(StudentInfo.class);
        job.setMapOutputValueClass(Text.class);

        //5.设置最终输出数据的key,value类型
        job.setOutputKeyClass(StudentInfo.class);
        job.setOutputValueClass(NullWritable.class);

        //指定输出格式化类
        job.setOutputFormatClass(MyOutputFormat.class);

        //6.设置数据输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("./data/scoredata.txt"));
        //将写出数据成功的标志文件写出到该目录
        FileOutputFormat.setOutputPath(job, new Path("./tmp/customreformat/" + System.currentTimeMillis() + "/output"));

        //7.运行任务
        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.println("任务执行成功！");
        } else {
            System.out.println("任务执行失败！");
        }
    }
}
