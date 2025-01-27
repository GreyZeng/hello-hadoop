package git.snippets.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * @author <a href="mailto:410486047@qq.com">Grey</a>
 * @date 2025/01/27
 * @since 1.8
 */
public class TestHDFS {
    private static final String HDFS_URI = "hdfs://node01:8020";
    private static final String HDFS_USER = "root";
    public static FileSystem fs;

    static {
        try {
            fs = FileSystem.get(URI.create(HDFS_URI), new Configuration(true), HDFS_USER);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        //查看HDFS路径文件
        listHDFSPathDir("/");
        fs.close();
    }

    private static void listHDFSPathDir(String hdfsPath) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsPath));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath());
        }
    }
}
