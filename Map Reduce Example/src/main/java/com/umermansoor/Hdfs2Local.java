package com.umermansoor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Hdfs2Local {

	public static String separator = System.getProperty("line.separator");

	

	@SuppressWarnings("deprecation")
	public static void writeFile(String hdfsPath, String path, String areacode) {
		// String hdfsPath =
		// "hdfs://localhost:9000/user/output/wordcount/part-r-00000";//"F://result.txt"
		Configuration conf = new Configuration();
		FileSystem fs = null;
		FSDataInputStream in = null;
		FileWriter fw = null;
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf);
			fw = new FileWriter(path);
			int line = 1;
			in = fs.open(new Path(hdfsPath));
			String tempString = null;
			fw.write("var arr" + areacode + " = [" + separator);
			while ((tempString = in.readLine()) != null) {
				// 显示行号
				if (!"".equals(tempString.trim())) {
//					if (line <= 1000) {//提取前1000行数据
						if(line == 1){
							//AppKudu.data
							fw.write(tempString.substring(0, tempString.length()-1) + separator);
						}else{
							fw.write(","+tempString.substring(0, tempString.length()-1) + separator);
						}
//					}
					line++;
				}
			}
			fw.write("]" + separator);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (in != null) {
					in.close();
				}
				if (fw != null) {
					fw.close();
				}
				if (fs != null) {
					fs.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

	}

	@SuppressWarnings("unused")
	private static void writeHdfsToLocalFile() {
		String[] contents = new String[] {
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				"dddddddddddddddddddddddddddddddd",
				"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", };
		String file = "hdfs://localhost:9000/user/data/test.log";
		Path path = new Path(file);
		Configuration conf = new Configuration();
		FileSystem fs = null;
		FSDataOutputStream output = null;
		try {
			fs = path.getFileSystem(conf);
			output = fs.create(path); // 创建文件
			for (String line : contents) { // 写入数据
				output.write(line.getBytes("UTF-8"));
				output.flush();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				output.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		//writeHdfsToLocalFile();
	}

}
