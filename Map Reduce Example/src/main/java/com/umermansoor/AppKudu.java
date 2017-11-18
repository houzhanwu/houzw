package com.umermansoor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.mapreduce.KuduTableMapReduceUtil;

import com.util.Config;
import com.util.DateUtil;
import com.util.PropertiesUnit;

/**
 * 
 * @ClassName: AppKudu
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author songwei
 * @date 2017年6月19日 下午6:21:43
 * 
 */
public class AppKudu {
	// kudu
	private String COLUMN_PROJECTION_KEY;
	private String MASTER_ADDRESSES_KEY;
	// private String VEHICLENO_GREATER;
	// private String VEHICLENO_LESS;
	
	public static Map<String, Integer> data = null;

	// mapreduce
	@SuppressWarnings("unused")
	private int NumReduceTasks;
	public static String fileSeparator = File.separator;
	public static int THREADNUM = 0;

	public void init(String path) {
		InputStream fileInputStream = null;
		PropertiesUnit pu = null;
		try {
			// 加载service.properties配置文件
			pu = new PropertiesUnit(path);
		} catch (Exception e) {
			System.out.println("配置文件初始化失败!");
			System.out.println(e);
		} finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
		MASTER_ADDRESSES_KEY = pu.readValue("MASTER_ADDRESSES_KEY");
		COLUMN_PROJECTION_KEY = pu.readValue("COLUMN_PROJECTION_KEY");
		// VEHICLENO_GREATER = pu.readValue("VEHICLENO_GREATER");
		// VEHICLENO_LESS = pu.readValue("VEHICLENO_LESS");
		NumReduceTasks = Integer.parseInt(pu.readValue("NumReduceTasks"));
	}

	public void create(String path, String tableName) throws ClassNotFoundException, IOException, InterruptedException {
		Job jobForBJ = null;
		try {
			jobForBJ = getJob(tableName, "120000");
			while (!jobForBJ.waitForCompletion(true) 
					) {
				Thread.sleep(10000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	private Job getJob(String tableName, String areacode) throws IOException, KuduException, ParseException {
		String pathString = fileSeparator + "track" + fileSeparator + "final" + fileSeparator + "20170620"
				+ fileSeparator + areacode + fileSeparator;
		Path outputDir = new Path(pathString);
		Job job = null;
		String jobName = "track_data_" + tableName ;
		Configuration conf = new Configuration();
		job = new Job(conf, jobName);
		Class<AppKudu> appKuduClass = AppKudu.class;
		job.setJarByClass(appKuduClass);
		job.setMapperClass(EarthquakeMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(EarthquakeReducer.class);

		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		KuduClient client = new KuduClient.KuduClientBuilder(MASTER_ADDRESSES_KEY).build();
		KuduTable table = null;
		table = client.openTable(tableName);
		long startTime = DateUtil.getStartTime("20170620");
		long endTime = DateUtil.getEndTime("20170620");
		Schema schema = table.getSchema();
		KuduPredicate positiontimeKpStart = KuduPredicate.newComparisonPredicate(schema.getColumn("positiontime"),
				KuduPredicate.ComparisonOp.GREATER, startTime);// 创建predicate
		KuduPredicate positiontimekpEnd = KuduPredicate.newComparisonPredicate(schema.getColumn("positiontime"),
				KuduPredicate.ComparisonOp.LESS, endTime);// 创建predicate
//		KuduPredicate vehiclenoKpStart = KuduPredicate.newComparisonPredicate(schema.getColumn("vehicleno"),
//				KuduPredicate.ComparisonOp.GREATER, Config.getVINMAP(areacode).split(",")[0]);// 创建predicate
//		KuduPredicate vehiclenokpEnd = KuduPredicate.newComparisonPredicate(schema.getColumn("vehicleno"),
//				KuduPredicate.ComparisonOp.LESS, Config.getVINMAP(areacode).split(",")[1]);// 创建predicate

		// KuduPredicate vehiclenoKpStart = KuduPredicate
		// .newComparisonPredicate(schema.getColumn("trans"),
		// KuduPredicate.ComparisonOp.EQUAL, 30);// 创建predicate
		new KuduTableMapReduceUtil.TableInputFormatConfigurator(job, tableName, COLUMN_PROJECTION_KEY,
				MASTER_ADDRESSES_KEY).addPredicate(positiontimeKpStart).addPredicate(positiontimekpEnd).configure();
//						.addPredicate(vehiclenoKpStart).addPredicate(vehiclenokpEnd).configure();
		// .addPredicate(vehiclenoKpStart).configure();

		try {
			if (job.waitForCompletion(true)) {
				// hdfs://nameservice1/track/data/20170723/110000/part-r-00000
				// /home/tomcat/webapps/data-map/javascript //
				// /home/map-reduce/data
				String path = fileSeparator + "home" + fileSeparator + "heat-china" + fileSeparator + "final";
				String fileName = areacode + ".js";
				File file = new File(path);
				if (!file.exists()) {
					file.mkdirs();
				}
				File file1 = new File(path + fileSeparator + fileName);
				if (!file1.exists()) {
					file1.createNewFile();
				}else {
					file1.delete();
					file1.createNewFile();
				}
				
				
				
				String hdfsPath = "hdfs://nameservice1" + pathString + fileSeparator + "part-r-00000";
				Hdfs2Local.writeFile(hdfsPath, path + fileSeparator + fileName, areacode);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return job;
	}

	public static void main(String[] args)
			throws ClassNotFoundException, IOException, InterruptedException, ParseException {
		AppKudu ak = new AppKudu();
		String path = null;
		String tableName = null;
		if (args.length == 1) {
			tableName = DateUtil.getTableName("20170620");
			path = args[0];
		} else if (args.length == 2) {
			path = args[0];
			tableName = args[1];
		} else {
			System.exit(-1);
		}
		data = readFileByLines("/home/heat/data.txt");
		ak.init(path);
		// Create the job specification object
		ak.create(path, tableName);
	}
	
	public static Map<String, Integer> readFileByLines(String fileName) {
		Map<String, Integer> data = new HashMap<String, Integer>();
		File file = new File(fileName);
		BufferedReader reader = null;
		try {
			System.out.println("以行为单位读取文件内容，一次读一整行：");
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int line = 1;
			// 一次读入一行，直到读入null为文件结束
			while ((tempString = reader.readLine()) != null) {
				// 显示行号
				data.put(tempString.split(" ")[0].trim(), Integer.parseInt(tempString.split(" ")[1].trim()));
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
		System.out.println("过滤数据文件初始化成功！"+data.size());
		return data;
	}
}
