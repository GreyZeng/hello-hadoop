package git.snippets.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

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
    public static final String DIR_PATH = "/" + System.currentTimeMillis();
    public static final String FILE_TO_HDFS = "./data/test.txt";
    public static final String FILE_ON_DIR_PATH = DIR_PATH + "/test.txt";
    public static final String FILE_NEW_NAME_ON_HDFS = DIR_PATH + "/hello.txt";
    public static final FileSystem FS;

    static {
        try {
            FS = FileSystem.get(URI.create(HDFS_URI), new Configuration(true), HDFS_USER);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("=====首先把所有数据清空=====");
        deleteHDFSFiles("/");
        System.out.println("=====数据清空完毕====");
        //查看HDFS路径文件
        listHDFSFiles("/");
        mkdirOnHDFS(DIR_PATH);
        System.out.println("======创建后的文件列表==========");
        listHDFSFiles("/");
        System.out.println("=============================");
        System.out.println("上传" + FILE_TO_HDFS + "这个文件到" + FILE_ON_DIR_PATH);
        writeFileToHDFS(FILE_TO_HDFS, FILE_ON_DIR_PATH);
        System.out.println("上传" + FILE_TO_HDFS + "这个文件到" + FILE_ON_DIR_PATH + "之后，文件列表清单如下");
        listHDFSFiles("/");
        System.out.println("=============================");
        System.out.println("====读取" + FILE_ON_DIR_PATH + "中的内容==========");
        readFileFromHDFS(FILE_ON_DIR_PATH);
        System.out.println("将" + FILE_ON_DIR_PATH + "重命名为：" + FILE_NEW_NAME_ON_HDFS);
        renameHDFSFile(FILE_ON_DIR_PATH, FILE_NEW_NAME_ON_HDFS);
        System.out.println("====读取" + FILE_NEW_NAME_ON_HDFS + "中的内容==========");
        readFileFromHDFS(FILE_NEW_NAME_ON_HDFS);
        System.out.println("获取" + FILE_NEW_NAME_ON_HDFS + "中的元数据信息");
        getHDFSFileInfos(FILE_NEW_NAME_ON_HDFS);
        System.out.println("======现在的文件清单是=====");
        listHDFSFiles("/");
        System.out.println("======清空所有文件后，清单为====");
        deleteHDFSFiles("/");
        listHDFSFiles("/");
        FS.close();
    }

    // 创建目录
    private static void mkdirOnHDFS(String dirPath) throws IOException {
        Path path = new Path(dirPath);
        if (FS.exists(path)) {
            System.out.println("当前" + dirPath + "已经存在！");
            return;
        }
        //创建HDFS目录
        boolean result = FS.mkdirs(path);
        if (result) {
            System.out.println("创建" + dirPath + "成功！");
        } else {

            System.out.println("创建" + dirPath + "失败！");
        }
    }

    // 递归删除所有的文件和文件名
    private static void deleteHDFSFiles(String hdfsPath) throws IOException {
        FileStatus[] fileStatuses = FS.listStatus(new Path(hdfsPath));
        for (FileStatus fileStatus : fileStatuses) {
            FS.delete(fileStatus.getPath(), true);
        }
    }

    // 递归列出所有的文件和文件名
    private static void listHDFSFiles(String hdfsPath) throws IOException {
        FileStatus[] fileStatuses = FS.listStatus(new Path(hdfsPath));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()) {
                listHDFSFiles(fileStatus.getPath().toString());
            }
            System.out.println(fileStatus.getPath());
        }
    }

    // 写文件到hdfs
    private static void writeFileToHDFS(String localFilePath, String hdfsFilePath) throws IOException {
        Path path = new Path(hdfsFilePath);
        if (FS.exists(path)) {
            FS.delete(path, true);
        }
        FS.copyFromLocalFile(false, true, new Path(localFilePath), path);
        System.out.println("上传文件成功！");
    }

    private static void readFileFromHDFS(String hdfsFilePath) throws IOException {
        //读取HDFS文件数据
        Path path = new Path(hdfsFilePath);
        FSDataInputStream in = FS.open(path);
        BufferedReader br;
        br = new BufferedReader(new InputStreamReader(in));
        String newLine;
        while ((newLine = br.readLine()) != null) {
            System.out.println(newLine);

        }
        br.close();
        in.close();
    }

    private static void getHDFSFileInfos(String hdfsFilePath) throws IOException {
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

    private static void renameHDFSFile(String hdfsOldFileName, String hdfsNewFileName) throws IOException {
        FS.rename(new Path(hdfsOldFileName), new Path(hdfsNewFileName));
        System.out.println("重命名成功！");
    }
}
