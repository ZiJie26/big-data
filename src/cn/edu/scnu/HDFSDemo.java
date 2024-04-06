package cn.edu.scnu;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class HDFSDemo {
	// 从本地上传文件到HDFS
	@Test
	public void test() throws IOException, InterruptedException, URISyntaxException {
		// 引入环境，是hadoop包中的Configuration; 配置文件要和linux虚拟机中一致；
		Configuration conf = new Configuration();
		// 在代码中指定的配置优先于xml中的配置。
		conf.set("dfs.replication", "2");
		// 连接HDFS；URI:连接地址； 在java.net.URI包中；conf:是一个配置
		// 按照提示，引入异常处理
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.126.151:9000"), conf, "root");
		// 上传文件； 当前是src目录
//注意：先建好abc.txt文件；很多同学文件的后缀是abc.txt.txt;  文件：查看：扩展名；
//注意：这里abc.txt不要通过查看文件属性复制路径，因为这样在盘符C前面会包含一个不可见符号。会报错：Relative path in absolute URI

		fs.copyFromLocalFile(new Path("C:\\Users/ZiJie/bigdata/12.txt"), new Path("/abc.txt"));
//注意：/wgh12345/ :红色斜杠表示目录copy到wgh12345目录；没有红色斜杠则表示copy到根目录，文件名为wgh12345
//fs.copyFromLocalFile(new Path("C:\\BigData/abc.txt"), new Path("/wgh12345/"));
	}
	
	// 使用HDFS API在HDFS中创建文件
	@Test
	public void create() throws Exception {
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://192.168.126.151:9000");
		FileSystem fs = FileSystem.get(uri, conf, "hadoop");
		Path path = new Path("/czj/test-data.txt");
		FSDataOutputStream newFile = fs.create(path, true);
		newFile.writeBytes("hello");
		newFile.close();
		fs.close();
	}

	// 使用HDFS API读取HDFS中的文件
	@Test
	public void read() throws Exception {
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://192.168.126.151:9000");
		FileSystem fs = FileSystem.get(uri, conf, "hadoop");
		Path path = new Path("/czj/test-data.txt");
	        InputStream in = null;
	        try{
	            in = fs.open(path);
	            IOUtils.copyBytes(in, System.out, 2048, false);
	        }catch(Exception e){
	            IOUtils.closeStream(in);
	        }
	}
	
	// 将HDFS中的文件下载到本地
	@Test
	public void download() throws Exception {
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://192.168.126.151:9000");
		FileSystem fs = FileSystem.get(uri, conf, "hadoop");
		Path src = new Path("/czj/test-data.txt");
		Path dst = new Path("/root/data.txt");
		fs.copyToLocalFile(false, src, dst, true);
		fs.close();
		System.out.println("Download Successfully!");
	}

	// 删除hdfs中指定的文件
	@SuppressWarnings("deprecation")
	@Test
	public void delete() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.126.151:9000"), conf, "root");
		// fs.delete(new Path("/HDFSDemo.txt")); //不要用这个，用deleteOnExit
		Path path = new Path("/czj/Resume.pdf");
		boolean isok = fs.deleteOnExit(path);
		if (isok) {
			System.out.println("ok");
		} else {
			System.out.println("fail");
		}
	}
}
