package com.lmk.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.rmi.Remote;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class HDFSClient {
	
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException{
		
		Configuration conf =new Configuration();
		//conf.set("fs.defultFS", "hdfs://hadoop102:9000");
		//conf.set("dfs.client.use.datanode.hostname", "true");//添加此配置信息即可
		
		// 1获取hdfs客户端对象
		//FileSystem fs = FileSystem.get(conf);
		
		FileSystem fs=FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "LMK");
		
		//2 在hdfs上创建路径
		fs.mkdirs(new Path("/0529/liumk"));
		// 3关闭资源
		fs.close();		
		System.out.println("over");
	}
	
	@Test
	//测试一下文件上传
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		//1获取hdfs对象
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf , "LMK");
		
		//2执行上传API
		fs.copyFromLocalFile(new Path("F://eclipes_workplace/HDFS/testlmk.txt"), new Path("/liumktest.txt"));
		//3关闭资源
		fs.close();
	}
	
	@Test
	//测试文件下载
	public void testCopyToLocal() throws IOException, InterruptedException, URISyntaxException {
		//1获取hdfs对象
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf , "LMK");
		//2执行下载命令
		fs.copyToLocalFile(new Path("/0529/liumk/testlm.txt"), new Path("F:/"));
		//3关闭资源
		fs.close();
	}
	
	@Test
	//文件删除
	public void testDelete() throws IOException, InterruptedException, URISyntaxException {
		
		//1获取对象
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "LMK");
		//2文件删除
		fs.delete(new Path("/0529"),true);
		//3关闭资源
		fs.close();
	}
	
	
	@Test
	public void testRename() throws IOException, InterruptedException, URISyntaxException {
		//1获取文件对象
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "LMK");
		//2执行更名操作
		fs.rename(new Path("/liumktest.txt"), new Path("/lmktest.txt"));
		//3关闭资源
		fs.close();
	}
	
	@Test
	public void testListFiles() throws IOException, InterruptedException, URISyntaxException {
		//1获取文件系统
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"LMK");
		
		//获取文件详情
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		
		while(listFiles.hasNext()) {
			LocatedFileStatus status = listFiles.next();
			
			//s输出详情，文件名称
			System.out.println(status.getPath().getName());
			//输出长度
			System.out.println(status.getLen());
			//输出权限
			System.out.println(status.getPermission());
			//分组
			System.out.println(status.getGroup());
			
			//获取储存快的信息
			BlockLocation[] blockLocations = status.getBlockLocations();
			for(BlockLocation blockLocation:blockLocations) {
				String[] hosts = blockLocation.getHosts();
				for(String host:hosts) {
					System.out.println(host);
				}
			}
			System.out.println("**********");
	
		}
	}
		
		@Test
		public void testListStatus() throws IOException, InterruptedException, URISyntaxException{
				
			// 1 获取文件配置信息
			Configuration configuration = new Configuration();
			FileSystem fs= FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "LMK");
				
			// 2 判断是文件还是文件夹
			FileStatus[] listStatus = fs.listStatus(new Path("/"));
				
			for (FileStatus fileStatus : listStatus) {
				
				// 如果是文件
				if (fileStatus.isFile()) {
						System.out.println("f:"+fileStatus.getPath().getName());
					}else {
						System.out.println("d:"+fileStatus.getPath().getName());
					}
				}
				
			// 3 关闭资源
			fs.close();
		}
		
	
	
	
	
	
	
	
	
}
