package git.snippets.hdfs;

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.List;

import static git.snippets.hdfs.HDFSApi.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class HDFSApiTest {

    public static final String DIR_PATH = "/" + System.currentTimeMillis();// 创建的目录地址
    public static final String ROOT = "/"; // 根目录
    public static final String LOCAL_FILE_TO_HDFS = "./data/test.txt";
    public static final String REMOTE_FILE_ON_HDFS = DIR_PATH + "/test.txt";
    public static final String FILE_NEW_NAME_ON_HDFS = DIR_PATH + "/hello.txt";

    @BeforeAll
    @DisplayName("执行单元测试之前，把目录全部清空")
    static void beforeAll() throws IOException {
        cleanHDFSFold(ROOT);
    }

    @Order(1)
    @Test
    @DisplayName("测试清空文件夹")
    void testCleanHDFSFold() throws IOException {
        Assertions.assertTrue(cleanHDFSFold(ROOT));
    }

    @Order(2)
    @Test
    @DisplayName("测试新建一个文件夹")
    void testCreateHDFSFiles() throws IOException {
        Assertions.assertTrue(mkdirOnHDFS(DIR_PATH));
    }

    @Order(3)
    @Test
    @DisplayName("递归列出路径下所有文件和文件夹")
    void testListHDFSFiles() throws IOException {
        List<String> files = listHDFSFiles(ROOT);
        Assertions.assertNotNull(files);
        Assertions.assertEquals(1, files.size());
        System.out.println(files.get(0));
    }

    @Order(4)
    @Test
    @DisplayName("将某个文件上传到某个目录下，原先目录有这个文件则先删除再上传")
    void testWriteFileToHDFS() throws IOException {
        Assertions.assertTrue(writeFileToHDFS(LOCAL_FILE_TO_HDFS, REMOTE_FILE_ON_HDFS));
        List<String> result = listHDFSFiles(DIR_PATH);
        Assertions.assertEquals(1, result.size());
        System.out.println(result.get(0));
    }

    @Order(5)
    @Test
    @DisplayName("测试文件读取")
    void testReadFileFromHDFS() throws IOException {
        System.out.println("====文件内容如下===");
        System.out.print(readFileFromHDFS(REMOTE_FILE_ON_HDFS));
        System.out.println("=====文件的元信息如下====");
        getFileMetaData(REMOTE_FILE_ON_HDFS);
    }

    @Order(6)
    @Test
    @DisplayName("测试重命名文件")
    void testRenameHDFSFile() throws IOException {
        Assertions.assertTrue(renameHDFSFile(REMOTE_FILE_ON_HDFS, FILE_NEW_NAME_ON_HDFS));
        System.out.println("====重命名的文件内容如下===");
        System.out.print(readFileFromHDFS(FILE_NEW_NAME_ON_HDFS));
        System.out.println("=====文件的元信息如下====");
        getFileMetaData(FILE_NEW_NAME_ON_HDFS);
    }

    @AfterAll
    @DisplayName("执行完毕所有单元测试后，把目录全部清空")
    static void tearDownAll() throws IOException {
        cleanHDFSFold(ROOT);
        closeFS();
    }
}