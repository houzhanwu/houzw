/**  
 * @Title: Test.java
 * @Package com.umermansoor
 * @author houzwhouzw
 * @createdate 2017年8月15日
 * @Description: TODO
 * @function list:
 * @--------------------edit history-----------------
 * @editdate 2017年8月15日
 * @editauthor houzw
 * @Description TODO
 */
package com.umermansoor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * ClassName: Test
 * 
 * @author houzw
 * @date 2017年8月15日
 * @Description: TODO
 */
public class Test {
	public static String separator = System.getProperty("line.separator");

	public static Map<String, String> data = null;

	public static void main(String[] args) {

		data = readFile("F://data.txt");
		readFileByLines("F://part-r-00000","F://110000.js");
	}

	public static void readFileByLines(String fileName, String fileOut) {
		File file = new File(fileName);
		BufferedReader reader = null;
		FileWriter fw = null;
		int line = 1;
		try {
			fw = new FileWriter(fileOut);
			System.out.println("以行为单位读取文件内容，一次读一整行：");
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			fw.write("var arr" + 110000 + " = [" + separator);
			while ((tempString = reader.readLine()) != null) {
				
				
				if (tempString.split("#").length==2) {
					String vin = tempString.split("#")[0].trim().split("_")[0].trim();
					String color = tempString.split("#")[0].trim().split("_")[1].trim();
					System.out.println("-----------vin:"+vin+","+color);
					
					
					if (data.containsKey(vin)) {
						if ("0".equals(data.get(vin))) {
							System.out.println(tempString.split("#")[1].substring(0, tempString.split("#")[1].length() - 1));
							
							fw.write("," + tempString.split("#")[1].substring(0, tempString.split("#")[1].length() - 1)
									+ separator);
						} else {
							if (data.get(vin).equals(color)) {
								fw.write("," + tempString.split("#")[1].substring(0, tempString.split("#")[1].length() - 1)
										+ separator);
							}
						}
					}
				}
				
				line++;
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
	}

	
	
	
	public static Map<String, String> readFile(String fileName) {
		Map<String, String> data = new HashMap<String, String>();
		File file = new File(fileName);
		BufferedReader reader = null;
		try {
			System.out.println("以行为单位读取文件内容，一次读一整行：");
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int line = 1;
			// 一次读入一行，直到读入null为文件结束
			while ((tempString = reader.readLine()) != null) {
				data.put(tempString.split(" ")[0], tempString.split(" ")[1]);
				line++;
				if (line % 1000000 == 0) {
					System.out.println(tempString.split(" ")[0] + "--" + tempString.split(" ")[1]);
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
		System.out.println("过滤数据文件初始化成功！" + data.size());
		return data;
	}
	
	
	
	
	
	
}
