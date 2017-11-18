package com.oracletest;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.mapreduce.KuduTableMapReduceUtil;

import com.outformat.OracleDBOutputFormat;
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
public class AppKuduTest {
	// kudu
	private String COLUMN_PROJECTION_KEY;
	private String MASTER_ADDRESSES_KEY;

	// mapreduce
	private int NumReduceTasks;

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
		NumReduceTasks = Integer.parseInt(pu.readValue("NumReduceTasks"));
	}

	@SuppressWarnings("deprecation")
	public void create(String path, String tableName)
			throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("mapred.compress.map.output", "true");
		DBConfiguration.configureDB(conf, "oracle.jdbc.driver.OracleDriver",
				"jdbc:oracle:thin:@172.16.112.121:1521:rdt1", "dct",
				"q8dQz_cnqP");
		String areacode = null;
		// areacode = "110000";
		String[] areacodes = { "110000", "650000" };
		// for (int i = 0; i < areacodes.length; i++) {
		// areacode = areacodes[i];
		Job job = null;
		String jobName = "offline_algorithm_" + tableName + "_" + areacode;
		try {
			job = new Job(conf, jobName);

			Class<AppKuduTest> appKuduClass = AppKuduTest.class;
			job.setJarByClass(appKuduClass);
			// Set the Mapper and Reducer classes
			job.setMapperClass(EarthquakeMapperTest.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(EarthquakeReducerTest.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(OracleDBOutputFormat.class);

			OracleDBOutputFormat.setOutput(job, "IIS_ANALYSIS_", "VEHICLENO",
					"PLATECOLOR", "ACCESSCODE", "TRANS", "INFO", "VEHICLENO",
					"PLATECOLOR", "VEHICLENO", "PLATECOLOR", "ACCESSCODE",
					"TRANS", "INFO");

			job.setNumReduceTasks(NumReduceTasks);
			for (int i = 0; i < areacodes.length; i++) {
				areacode = areacodes[i];
				// for (Map.Entry<String, Integer> entry :
				// Config.AREMAP.entrySet()) {
				// areacode = entry.getKey();
				KuduClient client = new KuduClient.KuduClientBuilder(
						MASTER_ADDRESSES_KEY).build();
				KuduTable table = null;
				table = client.openTable(tableName);
				long startTime = DateUtil.getStartTime("");
				long endTime = DateUtil.getEndTime("");
				Schema schema = table.getSchema();
				KuduPredicate positiontimeKpStart = KuduPredicate
						.newComparisonPredicate(
								schema.getColumn("positiontime"),
								KuduPredicate.ComparisonOp.GREATER, startTime);// 创建predicate
				KuduPredicate positiontimekpEnd = KuduPredicate
						.newComparisonPredicate(
								schema.getColumn("positiontime"),
								KuduPredicate.ComparisonOp.LESS, endTime);// 创建predicate
				KuduPredicate vehiclenoKpStart = KuduPredicate
						.newComparisonPredicate(schema.getColumn("vehicleno"),
								KuduPredicate.ComparisonOp.GREATER, Config
										.getVINMAP(areacode).split(",")[0]);// 创建predicate
				KuduPredicate vehiclenokpEnd = KuduPredicate
						.newComparisonPredicate(schema.getColumn("vehicleno"),
								KuduPredicate.ComparisonOp.LESS, Config
										.getVINMAP(areacode).split(",")[1]);// 创建predicate

				new KuduTableMapReduceUtil.TableInputFormatConfigurator(job,
						tableName, COLUMN_PROJECTION_KEY, MASTER_ADDRESSES_KEY)
						.addPredicate(positiontimeKpStart)
						.addPredicate(positiontimekpEnd)
						.addPredicate(vehiclenoKpStart)
						.addPredicate(vehiclenokpEnd).configure();
			}
			// }
		} catch (Exception e) {
			e.printStackTrace();
		}
		while (!job.waitForCompletion(true)) {
			Thread.sleep(10000);
		}
	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException, ParseException {
		AppKuduTest ak = new AppKuduTest();
		String path = null;
		String tableName = null;
		if (args.length == 1) {
			tableName = DateUtil.getTableName("");
			path = args[0];
		} else if (args.length == 2) {
			path = args[0];
			tableName = args[1];
		} else {
			System.exit(-1);
		}
		ak.init(path);
		// Create the job specification object
		ak.create(path, tableName);
	}
}
