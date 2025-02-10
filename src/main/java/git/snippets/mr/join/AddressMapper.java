package git.snippets.mr.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AddressMapper extends Mapper<LongWritable, Text,PersonInfo,PersonInfo> {
    PersonInfo personInfo = new PersonInfo();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PersonInfo, PersonInfo>.Context context) throws IOException, InterruptedException {
        //1,北京
        String[] split = value.toString().split(",");

        personInfo.setId(split[0]);
        personInfo.setName("");
        personInfo.setAge(0);
        personInfo.setAddress(split[1]);
        personInfo.setFlag("address");
        context.write(personInfo,personInfo);
    }
}
