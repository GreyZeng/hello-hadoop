package git.snippets.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:410486047@qq.com">Grey</a>
 * @date 2025/01/27
 * @since 1.8
 * 伪分布式 HDFS Java API 测试
 */
public class HDFSApi {
    // node01要配置本地的host文件
    private static final String HDFS_URI = "hdfs://node01:8020";
    private static final String HDFS_USER = "root";
    public static final FileSystem FS;

    static {
        try {
            FS = FileSystem.get(URI.create(HDFS_URI), new Configuration(true), HDFS_USER);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    // 递归删除所有的文件和文件名
    protected static boolean cleanHDFSFold(String foldPath) throws IOException {
        FileStatus[] fileStatuses;
        boolean result = true;
        fileStatuses = FS.listStatus(new Path(foldPath));
        if (null != fileStatuses) {
            for (FileStatus fileStatus : fileStatuses) {
                result = result && FS.delete(fileStatus.getPath(), true);
            }
        }
        return result;
    }


    // 创建目录
    protected static boolean mkdirOnHDFS(String dirPath) throws IOException {
        //创建HDFS目录
        return FS.mkdirs(new Path(dirPath));
    }


    // 递归列出所有的文件和文件名
    protected static List<String> listHDFSFiles(String hdfsPath) throws IOException {
        List<String> result = new ArrayList<>();
        FileStatus[] fileStatuses;
        fileStatuses = FS.listStatus(new Path(hdfsPath));
        if (null != fileStatuses) {
            for (FileStatus fileStatus : fileStatuses) {
                result.add(fileStatus.getPath().toString());
                if (fileStatus.isDirectory()) {
                    result.addAll(listHDFSFiles(fileStatus.getPath().toString()));
                }
            }

        }
        return result;
    }

    // 写文件到hdfs
    protected static boolean writeFileToHDFS(String localFilePath, String hdfsFilePath) throws IOException {
        Path path = new Path(hdfsFilePath);
        if (FS.exists(path)) {
            FS.delete(path, true);
        }
        FS.copyFromLocalFile(false, true, new Path(localFilePath), path);
        return true;
    }

    protected static String readFileFromHDFS(String hdfsFilePath) throws IOException {
        //读取HDFS文件数据
        Path path = new Path(hdfsFilePath);
        FSDataInputStream in = FS.open(path);
        BufferedReader br;
        br = new BufferedReader(new InputStreamReader(in));
        StringBuilder sb = new StringBuilder();
        String newLine;
        while ((newLine = br.readLine()) != null) {
            sb.append(newLine).append("\n");
        }
        br.close();
        in.close();
        return sb.toString();
    }

    protected static void getFileMetaData(String hdfsFilePath) throws IOException {
        Path path = new Path(hdfsFilePath);
        RemoteIterator<LocatedFileStatus> listFilesIterator = FS.listFiles(path, true);
        while (listFilesIterator.hasNext()) {
            LocatedFileStatus fileStatus = listFilesIterator.next();
            System.out.println("文件详细信息如下：");
            System.out.println("权限：" + fileStatus.getPermission());
            System.out.println("所有者：" + fileStatus.getOwner());
            System.out.println("大小：" + fileStatus.getLen());
            System.out.println("块大小：" + fileStatus.getBlockSize());
            System.out.println("文件名：" + fileStatus.getPath().getName());

            //获取块的详情
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {

                System.out.println("block信息：" + blockLocation);
            }
        }

    }

    protected static boolean renameHDFSFile(String hdfsOldFileName, String hdfsNewFileName) throws IOException {
        return FS.rename(new Path(hdfsOldFileName), new Path(hdfsNewFileName));
    }

    protected static void closeFS() throws IOException {
        FS.close();
    }
}
