package git.snippets.mr.mapjoin;

import git.snippets.mr.LocalConfigJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.创建配置及job对象
        Job job = LocalConfigJob.getLocalJob();

        //2.设置Driver驱动类
        job.setJarByClass(MapJoinDriver.class);

        //3.设置Mapper、Reducer对应的类
        job.setMapperClass(MapJoinMapper.class);

        //4.设置Mapper输出Key,Value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.设置最终输出数据的key,value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //6.设置缓存文件
        job.addCacheFile(new Path("./data/addressdata.txt").toUri());

        //6.设置数据输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("./data/persondata.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./tmp/" + System.currentTimeMillis()));

        //7.运行任务
        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.println("任务执行成功！");
        } else {
            System.out.println("任务执行失败！");
        }
    }
}
