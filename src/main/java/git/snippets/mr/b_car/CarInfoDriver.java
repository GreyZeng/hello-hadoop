package git.snippets.mr.b_car;

import git.snippets.mr.LocalConfigJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义序列化类案例
 */
public class CarInfoDriver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// 1.创建配置及job对象
		Job job = LocalConfigJob.getLocalJob();

		// 2.设置Driver驱动类
		job.setJarByClass(CarInfoDriver.class);

		// 3.设置Mapper、Reducer对应的类
		job.setMapperClass(CarInfoMapper.class);
		job.setReducerClass(CarInfoReducer.class);

		// 4.设置Mapper输出Key,Value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CarInfo.class);

		// 5.设置最终输出数据的key,value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CarInfo.class);

		// 6.设置数据输入和输出路径
		FileInputFormat.setInputPaths(job, new Path("data/cardata.txt"));
		FileOutputFormat.setOutputPath(job, new Path("./tmp/car/" + System.currentTimeMillis() + "/output"));

		// 7.运行任务
		boolean success = job.waitForCompletion(true);
		if (success) {
			System.out.println("任务执行成功！");
		} else {
			System.out.println("任务执行失败！");
		}
	}
}
