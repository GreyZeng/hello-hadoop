package git.snippets.mr.mapjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

// map端实现join，读取另外一个map的缓存
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    //job.addCacheFile(new Path("data/addressdata.txt").toUri());

    Map<String, String> addressMap = new HashMap<>();

    //在map方法调用前执行，先把addressMap初始化好
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        URI[] files = context.getCacheFiles();
        if (files != null) {
            for (URI file : files) {
                BufferedReader reader = new BufferedReader(new FileReader(file.getPath()));
                String line;
                while ((line = reader.readLine()) != null) {
                    //1,北京
                    String[] split = line.split(",");
                    addressMap.put(split[0], split[1]);
                }
                reader.close();
            }
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        //1,张三,18
        String[] split = line.split(",");
        String id = split[0];
        String name = split[1];
        int age = Integer.parseInt(split[2]);
        String address = addressMap.get(id);

        if (address != null) {
            String returnStr = "id=" + id + ",name=" + name + ",age=" + age + ",address=" + address;
            context.write(new Text(returnStr), NullWritable.get());
        }
    }
}
