package git.snippets.mr.car;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CarAnalyMapper extends Mapper<LongWritable,Text, Text,CarInfo> {
    //定义输出key
    private Text outputKey = new Text();

    //定义输出value
    private CarInfo outputValue = new CarInfo();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CarInfo>.Context context) throws IOException, InterruptedException {
        //1,京BXYWX,95,100
        String line = value.toString();
        String[] split = line.split(",");

        //设置key
        outputKey.set(split[1]);

        outputValue.setCar(split[1]);
        outputValue.setAvgSpeed(Double.valueOf(split[2]));
        outputValue.setTotalKm(Double.valueOf(split[3]));

        context.write(outputKey,outputValue);

    }
}
