package org.lyx.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

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
		conf.set("fs.replication", "3");
		FileSystem fileSystem = FileSystem.get(conf);

//		boolean b = fileSystem.exists(new Path("/hello.txt"));
//		System.out.println(b);
//
//		boolean success = fileSystem.mkdirs(new Path("/lyx"));
//		System.out.println(success);
//
//		b = fileSystem.exists(new Path("/lyx"));
//		System.out.println(b);
//		success = fileSystem.delete(new Path("/test.data"), true);
//		System.out.println(success);
//
//		b = fileSystem.exists(new Path("/lyx"));
//		System.out.println(b);

//		FSDataOutputStream out = fileSystem.create(new Path("/test.data"), true);
//		FileInputStream fis = new FileInputStream("d:/bigdata/test/test.txt");
//		IOUtils.copyBytes(fis, out, 4096, true);
//
//		FileStatus[] statuses = fileSystem.listStatus(new Path("/"));
//		//System.out.println(statuses.length);
//		for(FileStatus status : statuses) {
//		    System.out.println(status.getPath());
//		    System.out.println(status.getPermission());
//		    System.out.println(status.getReplication());
//		}

		//合并多个文件到HDFS中
		//uploadMultifile2OneFile(conf);
		//以下是压缩文件过时的方法
//		Path path = new Path("/lyx/seq");
//		SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem,conf,path,Text.class,Text.class);
//
//		File file = new File("D:\\bigdata\\seq");
//		for(File f : file.listFiles()) {
//			writer.append(new Text(f.getName()),new Text(FileUtils.readFileToString(f)));
//		}
		//下载压缩文件内容
		downloadMultifile(conf);
	}

	private static void uploadMultifile2OneFile(Configuration conf) throws IOException{
		Path path = new Path("/lyx/seq.dat");

		SequenceFile.Writer.Option pathOption = SequenceFile.Writer.file(path);
		SequenceFile.Writer.Option keyOption = SequenceFile.Writer.keyClass(Text.class);
		SequenceFile.Writer.Option valueOption = SequenceFile.Writer.valueClass(Text.class);
		//放入到writer中的key为文件名称，value为文件的内容
		SequenceFile.Writer writer = SequenceFile.createWriter(conf,pathOption,keyOption,valueOption);

		File file = new File("D:\\bigdata\\seq");
		for(File f : file.listFiles()) {
			writer.append(new Text(f.getName()),new Text(FileUtils.readFileToString(f)));
		}
	}

	private static void downloadMultifile(Configuration conf) throws IOException{
		Path path = new Path("/lyx/seq.dat");
		SequenceFile.Reader.Option pathOption = SequenceFile.Reader.file(path);
		//放入到writer中的key为文件名称，value为文件的内容
		SequenceFile.Reader reader = new SequenceFile.Reader(conf,pathOption);

		Text key = new Text();
		Text value = new Text();

		while(reader.next(key,value)) {
			System.out.println("fileName:" + key + ",fileValue:\n" + value);
			System.out.println("=======================================");
		}
	}

}
