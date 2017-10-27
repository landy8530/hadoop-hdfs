package org.lyx.hdfs;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

/**
 * 版权所有：厦门欣汇峰贸易有限公司
 *====================================================
 * 文件名称: org.lyx.hdfs.HelloHDFS.java
 * 修订记录：
 * No    日期				作者(操作:具体内容)
 * 1.    2017年4月23日			liuyuanxian(创建:创建文件)
 *====================================================
 * 类描述：(说明未实现或其它不应生成javadoc的内容)
 * 
 */
public class HelloHDFS {

	public static void main(String[] args) throws Exception {
//		URL url = new URL("http://www.baidu.com");
//		InputStream in = url.openStream();
//		IOUtils.copyBytes(in, System.out, 1024,true);
//		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
//		URL url = new URL("hdfs://192.168.56.200:9000/hello.txt");
//		InputStream in = url.openStream();
//		IOUtils.copyBytes(in, System.out, 1024,true);
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.56.200:9000");
		conf.set("fs.replication", "2");
		FileSystem fileSystem = FileSystem.get(conf);

//		boolean b = fileSystem.exists(new Path("/hello.txt"));
//		System.out.println(b);
//
//		boolean success = fileSystem.mkdirs(new Path("/lyx"));
//		System.out.println(success);
//
//		b = fileSystem.exists(new Path("/lyx"));
//		System.out.println(b);
//
//		success = fileSystem.delete(new Path("/lyx"), true);
//		System.out.println(success);
//
//		b = fileSystem.exists(new Path("/lyx"));
//		System.out.println(b);

		FSDataOutputStream out = fileSystem.create(new Path("/test.data"), true);
		FileInputStream fis = new FileInputStream("d:/bigdata/test/test.txt");
		IOUtils.copyBytes(fis, out, 4096, true);

		FileStatus[] statuses = fileSystem.listStatus(new Path("/"));
		//System.out.println(statuses.length);
		for(FileStatus status : statuses) {
		    System.out.println(status.getPath());
		    System.out.println(status.getPermission());
		    System.out.println(status.getReplication());
		}
		
	}

}
