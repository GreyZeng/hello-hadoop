package git.snippets.mr.customformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ScoreMapper extends Mapper<LongWritable, Text,StudentInfo,Text> {
    StudentInfo studentInfo = new StudentInfo();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, StudentInfo, Text>.Context context) throws IOException, InterruptedException {
        //zhangsan,20
        String line = value.toString();
        String[] split = line.split(",");
        studentInfo.setName(split[0]);
        studentInfo.setScore(Integer.valueOf(split[1]));

        context.write(studentInfo,value);

    }


}
