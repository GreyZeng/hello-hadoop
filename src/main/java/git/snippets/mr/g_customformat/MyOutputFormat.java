package git.snippets.mr.g_customformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义输出格式化类
 */
public class MyOutputFormat extends FileOutputFormat<StudentInfo, NullWritable> {
    @Override
    public RecordWriter<StudentInfo, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MyRecordWriter(context);
    }
}

/**
 * RecoreWriter 实现将Reducer 输出的 k,v 写出
 */
class MyRecordWriter extends RecordWriter<StudentInfo, NullWritable> {

    FSDataOutputStream passOutputStream;
    FSDataOutputStream failOutputStream;

    public MyRecordWriter(TaskAttemptContext context) throws IOException {
        //创建FileSystem
        FileSystem fileSystem = LocalFileSystem.getLocal(context.getConfiguration());
        //创建输出流 - 针对写出的不同文件都要创建 ，pass.txt ,fail.txt
        long fold = System.currentTimeMillis();
        passOutputStream = fileSystem.create(new Path("./tmp/customformat_out/" + fold + "/pass.txt"));
        failOutputStream = fileSystem.create(new Path("./tmp/customformat_out/" + fold + "/fail.txt"));
    }

    //将数据写出到文件
    @Override
    public void write(StudentInfo key, NullWritable value) throws IOException, InterruptedException {
        int score = key.getScore();
        if (score >= 80) {
            passOutputStream.writeBytes(score + "\n");
        } else {
            failOutputStream.writeBytes(score + "\n");
        }
    }

    //关闭资源-关闭写出的流对象
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        //关闭流对象
        IOUtils.closeStreams(passOutputStream, failOutputStream);
    }
}
