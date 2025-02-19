package git.snippets.mr.g_customformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ScoreReducer extends Reducer<StudentInfo, Text, StudentInfo, NullWritable> {
    @Override
    protected void reduce(StudentInfo key, Iterable<Text> values, Reducer<StudentInfo, Text, StudentInfo, NullWritable>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, NullWritable.get());
        }
    }
}
