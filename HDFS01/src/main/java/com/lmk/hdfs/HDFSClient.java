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
		//conf.set("dfs.client.use.datanode.hostname", "true");//��Ӵ�������Ϣ����
		
		// 1��ȡhdfs�ͻ��˶���
		//FileSystem fs = FileSystem.get(conf);
		
		FileSystem fs=FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "LMK");
		
		//2 ��hdfs�ϴ���·��
		fs.mkdirs(new Path("/0529/liumk"));
		// 3�ر���Դ
		fs.close();		
		System.out.println("over");
	}
	
	@Test
	//����һ���ļ��ϴ�
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		//1��ȡhdfs����
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf , "LMK");
		
		//2ִ���ϴ�API
		fs.copyFromLocalFile(new Path("F://eclipes_workplace/HDFS/testlmk.txt"), new Path("/liumktest.txt"));
		//3�ر���Դ
		fs.close();
	}
	
	@Test
	//�����ļ�����
	public void testCopyToLocal() throws IOException, InterruptedException, URISyntaxException {
		//1��ȡhdfs����
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf , "LMK");
		//2ִ����������
		fs.copyToLocalFile(new Path("/0529/liumk/testlm.txt"), new Path("F:/"));
		//3�ر���Դ
		fs.close();
	}
	
	@Test
	//�ļ�ɾ��
	public void testDelete() throws IOException, InterruptedException, URISyntaxException {
		
		//1��ȡ����
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "LMK");
		//2�ļ�ɾ��
		fs.delete(new Path("/0529"),true);
		//3�ر���Դ
		fs.close();
	}
	
	
	@Test
	public void testRename() throws IOException, InterruptedException, URISyntaxException {
		//1��ȡ�ļ�����
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "LMK");
		//2ִ�и�������
		fs.rename(new Path("/liumktest.txt"), new Path("/lmktest.txt"));
		//3�ر���Դ
		fs.close();
	}
	
	@Test
	public void testListFiles() throws IOException, InterruptedException, URISyntaxException {
		//1��ȡ�ļ�ϵͳ
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"LMK");
		
		//��ȡ�ļ�����
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		
		while(listFiles.hasNext()) {
			LocatedFileStatus status = listFiles.next();
			
			//s������飬�ļ�����
			System.out.println(status.getPath().getName());
			//�������
			System.out.println(status.getLen());
			//���Ȩ��
			System.out.println(status.getPermission());
			//����
			System.out.println(status.getGroup());
			
			//��ȡ��������Ϣ
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
				
			// 1 ��ȡ�ļ�������Ϣ
			Configuration configuration = new Configuration();
			FileSystem fs= FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "LMK");
				
			// 2 �ж����ļ������ļ���
			FileStatus[] listStatus = fs.listStatus(new Path("/"));
				
			for (FileStatus fileStatus : listStatus) {
				
				// ������ļ�
				if (fileStatus.isFile()) {
						System.out.println("f:"+fileStatus.getPath().getName());
					}else {
						System.out.println("d:"+fileStatus.getPath().getName());
					}
				}
				
			// 3 �ر���Դ
			fs.close();
		}
		
	
	
	
	
	
	
	
	
}
