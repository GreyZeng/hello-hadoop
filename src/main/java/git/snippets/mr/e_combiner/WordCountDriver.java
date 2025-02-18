package git.snippets.mr.e_combiner;

import git.snippets.mr.LocalConfigJob;
import git.snippets.mr.a_wordcount.WordCountMapper;
import git.snippets.mr.a_wordcount.WordCountReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Combiner是一个可选的优化步骤，在Map任务输出结果后、Reduce输入前执行。其作用是对Map任务的输出进行局部合并，将具有相同键的键值对合并为一个，以减少需要传输到Reduce节点的数据量，降低网络开销，并提高整体性能。
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
        job.setCombinerClass(WordCountReducer.class);

        //6.设置数据输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("./data/data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./tmp/combiner/" + System.currentTimeMillis() + "/output8"));

        //7.运行任务
        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.println("任务执行成功！");
        } else {
            System.out.println("任务执行失败！");
        }
    }
}
