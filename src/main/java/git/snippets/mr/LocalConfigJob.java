package git.snippets.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class LocalConfigJob {

    public static Job getLocalJob() throws IOException {
        // 由于resource目录下有配置文件了，这里并不是load resource下的配置文件，所以要把参数设置为false
        Configuration conf = new Configuration(false);
        conf.set("mapreduce.framework.name", "local"); // 使用本地模式
        conf.set("fs.defaultFS", "file:///"); // 使用本地文件系统
        return Job.getInstance(conf);
    }
}
