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
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.text.StyledEditorKit.ForegroundAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * ClassName: Test
 * 
 * @author houzw
 * @date 2017年8月15日
 * @Description: TODO
 */
public class HeatTest {
	public static String separator = System.getProperty("line.separator");

	public static Map<String, String> data = null;

	public static void main(String[] args) throws Exception {
		// 1--------
		// data = readFile("F://data.txt");//130w数据
		// readFileByLines("F://part-r-00000","F://vehposition.txt");

		// 2--------
		Map<String, Long> data = new HashMap<String, Long>();
		List<String> list = readVehFile("F://vehposition.txt") ;
		double lon = 0;
		double lat = 0;
		for (String string : list) {
			//113.36126499999999_23.305460999999998
			System.out.println(string );
			String longtitude = string.split("_")[0];
			String lattitude = string.split("_")[1];
			longtitude = longtitude.substring(0, longtitude.indexOf(".")-3);
			lattitude = lattitude.substring(0, lattitude.indexOf(".")-3);
			//----lon:1151569,lat368058
			lon = Double.parseDouble(longtitude);
			lat = Double.parseDouble(lattitude);
			String key = lon+"_"+lat;
			if (data.containsKey(key)) {
				data.put(key, data.get(key)+1);
			}else{
				data.put(key, 1L);
			}
			
		}
		
		
		write2Hdfs("F://data/data.js", data);
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
			// fw.write("var arr" + 110000 + " = [" + separator);
			while ((tempString = reader.readLine()) != null) {

				if (tempString.split("#").length == 2) {
					String vin = tempString.split("#")[0].trim().split("_")[0].trim();
					String color = tempString.split("#")[0].trim().split("_")[1].trim();
					System.out.println("-----------vin:" + vin + "," + color);
					if (data.containsKey(vin)) {
						if ("0".equals(data.get(vin))) {
							// System.out.println(tempString.split("#")[1].substring(0,
							// tempString.split("#")[1].length() - 1));

							fw.write(tempString.split("#")[1].substring(1, tempString.split("#")[1].length() - 2)
									+ separator);
						} else {
							if (data.get(vin).equals(color)) {
								fw.write(tempString.split("#")[1].substring(1, tempString.split("#")[1].length() - 2)
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

	public static List<String>  readVehFile(String fileName) {

		List<String> list = new ArrayList<String>();
		File file = new File(fileName);
		BufferedReader reader = null;
		try {
			// System.out.println("以行为单位读取文件内容，一次读一整行：");
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int line = 1;
			// 一次读入一行，直到读入null为文件结束
			while ((tempString = reader.readLine()) != null) {
				String[] points = tempString.split(",");
				//1123217.61,376000.1,4.89,3.31,2.75,1.99,5.01,-0.65,
				double lon = 0;
				double lat = 0;
				for (int i = 0; i < points.length - 1; i++) {
					if (i == 0) {
						lon = Double.parseDouble(points[i]);
						lat = Double.parseDouble(points[i + 1]);
						list.add(lon + "_" + lat);
						i = i+2;
					} else {
						double lng = Double.parseDouble(points[i]);
						double lati = Double.parseDouble(points[i + 1]);
						list.add((lon + lng) + "_" + (lat + lati));
						i = i+2;
					}

				}

//				line++;
//				if (line % 10 == 0) {
//					System.out.println(list.get(line));
//				}
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
		return list;
	}
	
	
	
	private static void write2Hdfs(String path, Map<String, Long> count)
			throws UnsupportedEncodingException, IOException {
		int num = 0;
		OutputStream outputStream = new FileOutputStream(new File(path));
		outputStream.write(("var points = [ \r\n").getBytes("UTF-8"));
		for (String key : count.keySet()) {
			Long value = count.get(key);
			num++;
			if (num % 20000 == 0) {
				System.out.println("Key = " + key + ", Value = " + value);
			}
			// out.write((key + "," + value +
			// "\r\n").getBytes("UTF-8"));// 118113_36838,1
			double lng = Double.parseDouble(key.split("_")[0]) / 10d;
			double lat = Double.parseDouble(key.split("_")[1]) / 10d;
			outputStream.write(
					("{\"lng\":" + lng + ",\"lat\":" + lat + ",\"count\":" + value + "},\r\n").getBytes("UTF-8"));// 118113_36838,1
		}
		outputStream.write(("] \r\n").getBytes("UTF-8"));
		if (outputStream != null) {
			outputStream.flush();
			outputStream.close();
		}

	}

}
