import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClientTest {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 初始化
        Configuration configuration = new Configuration();
        // 配置文件优先级： 代码设置 > ClassPath下的hdfs-site.xml > $HADOOP_HOME/etc/hadoop/hdfs-site.xml > hdfs-default.xml
        configuration.set("dfs.replication", "2");
        fs = FileSystem.get(new URI("hdfs://localhost111:8020"), configuration,"root");
    }

    @After
    public void close() throws IOException {
        // 资源关闭
        fs.close();
    }

    @Test
    public void testMkdirs() throws IOException {
        // 创建目录
        fs.mkdirs(new Path("/xiyou/huaguoshan2/"));
    }

    @Test
    public void testPut() throws IOException {
        // 测试文件上传
        fs.copyFromLocalFile(false, false, new Path("D:\\Project\\HdfsClientDemo\\src\\test\\resources\\sunwukong"), new Path("hdfs://localhost111:8020/xiyou/huaguoshan"));
    }

    @Test
    public void testGet() throws IOException {
        // 测试文件下载， 第四个参数表示： 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/xiyou/huaguoshan/sunwukong"), new Path("D:\\Project\\HdfsClientDemo\\src\\test\\resources\\sunwukong2"), true);
    }

    @Test
    public void testModify() throws IOException {
        // 修改文件名称
        fs.rename(new Path("/xiyou/huaguoshan/sunwukong"), new Path("/xiyou/huaguoshan/meihouwang.txt"));
    }

    @Test
    public void testDeleteFile() throws IOException {
        // 测试删除文件
        fs.delete(new Path("/xiyou/huaguoshan/meihouwang.txt"), true);
    }

    @Test
    public void testDeleteDir() throws IOException {
        // 测试删除目录
        fs.delete(new Path("/xiyou"), true);
    }

    @Test
    public void testGetFileDetail() throws IOException {
        // 测试文件详情获取
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    @Test
    public void testDetermineFileOrDir() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
    }
}