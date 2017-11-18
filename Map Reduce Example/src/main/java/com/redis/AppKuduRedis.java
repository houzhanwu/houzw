package com.redis;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.mapreduce.KuduTableMapReduceUtil;

import com.outformat.LoginLogOutputFormat;
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
public class AppKuduRedis {
	// kudu
	private String COLUMN_PROJECTION_KEY;
	private String MASTER_ADDRESSES_KEY;

	// mapreduce
	private int NumReduceTasks;
	private String Mapreduce_Reduce_Shuffle_Memory_Limit_Percent;
	private String Mapred_Child_Java_Opts;
	private String Mapred_Tasktracker_Map_Tasks_Maximum;
	private String Mapred_Tasktracker_Reduce_Tasks_Maximum;
	private String Mapreduce_Job_Reduce_Slowstart_Completedmaps;
	private String Yarn_App_Mapreduce_Am_Job_Reduce_Rampup_Limit;

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
		Mapreduce_Reduce_Shuffle_Memory_Limit_Percent = pu.readValue("Mapreduce_Reduce_Shuffle_Memory_Limit_Percent");
		Mapred_Child_Java_Opts = pu.readValue("Mapred_Child_Java_Opts");
		Mapred_Tasktracker_Map_Tasks_Maximum = pu.readValue("Mapred_Tasktracker_Map_Tasks_Maximum");
		Mapred_Tasktracker_Reduce_Tasks_Maximum = pu.readValue("Mapred_Tasktracker_Reduce_Tasks_Maximum");
		Mapreduce_Job_Reduce_Slowstart_Completedmaps = pu.readValue("Mapreduce_Job_Reduce_Slowstart_Completedmaps");
		Yarn_App_Mapreduce_Am_Job_Reduce_Rampup_Limit = pu.readValue("Yarn_App_Mapreduce_Am_Job_Reduce_Rampup_Limit");
	}

	@SuppressWarnings("deprecation")
	public void create(String path, String tableName, String dateday, String areacode) throws ClassNotFoundException, IOException, InterruptedException {
		Job job = null;
		Configuration conf = new Configuration();
		conf.set("mapreduce.reduce.shuffle.memory.limit.percent", Mapreduce_Reduce_Shuffle_Memory_Limit_Percent);
		conf.set("mapreduce.child.java.opts", Mapred_Child_Java_Opts);
		conf.set("mapreduce.tasktracker.map.tasks.maximum", Mapred_Tasktracker_Map_Tasks_Maximum);
		conf.set("mapreduce.tasktracker.reduce.tasks.maximum", Mapred_Tasktracker_Reduce_Tasks_Maximum);
		conf.set("mapreduce.job.reduce.slowstart.completedmaps", Mapreduce_Job_Reduce_Slowstart_Completedmaps);
		conf.set("yarn.app.mapreduce.am.job.reduce.rampup.limit", Yarn_App_Mapreduce_Am_Job_Reduce_Rampup_Limit);
		conf.set("mapreduce.map.memory.mb", "4096");
		try {
			String jobName = "offline_algorithm_" + tableName;
			job = new Job(conf, jobName);
			Class<AppKuduRedis> appKuduClass = AppKuduRedis.class;
			job.setJarByClass(appKuduClass);

			// Set the Mapper and Reducer classes
			job.setMapperClass(EarthquakeMapperRedis.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(EarthquakeReducerRedis.class);

			LoginLogOutputFormat.setOutputPath(job);
			job.setOutputFormatClass(LoginLogOutputFormat.class);
			// 设置输出key类型
			job.setOutputKeyClass(Text.class);
			// 设置输出value类型
			job.setOutputValueClass(Text.class);

			job.setNumReduceTasks(NumReduceTasks);

			KuduClient client = new KuduClient.KuduClientBuilder(MASTER_ADDRESSES_KEY).build();
			KuduTable table = null;

			table = client.openTable(tableName);

			long startTime = DateUtil.getStartTime(dateday);
			long endTime = DateUtil.getEndTime(dateday);

			Schema schema = table.getSchema();
			KuduPredicate positiontimeKpStart = KuduPredicate.newComparisonPredicate(schema.getColumn("positiontime"), KuduPredicate.ComparisonOp.GREATER, startTime);// 创建predicate
			KuduPredicate positiontimekpEnd = KuduPredicate.newComparisonPredicate(schema.getColumn("positiontime"), KuduPredicate.ComparisonOp.LESS, endTime);// 创建predicate

			if ("".equals(areacode)) {
				new KuduTableMapReduceUtil.TableInputFormatConfigurator(job, tableName, COLUMN_PROJECTION_KEY, MASTER_ADDRESSES_KEY).addPredicate(positiontimeKpStart)
						.addPredicate(positiontimekpEnd).configure();
			} else {
				KuduPredicate vehiclenoKpStart = KuduPredicate.newComparisonPredicate(schema.getColumn("vehicleno"), KuduPredicate.ComparisonOp.GREATER, Config.getVINMAP(areacode)
						.split(",")[0]);// 创建predicate
				KuduPredicate vehiclenokpEnd = KuduPredicate.newComparisonPredicate(schema.getColumn("vehicleno"), KuduPredicate.ComparisonOp.LESS, Config.getVINMAP(areacode)
						.split(",")[1]);// 创建predicate

				new KuduTableMapReduceUtil.TableInputFormatConfigurator(job, tableName, COLUMN_PROJECTION_KEY, MASTER_ADDRESSES_KEY).addPredicate(positiontimeKpStart)
						.addPredicate(positiontimekpEnd).addPredicate(vehiclenoKpStart).addPredicate(vehiclenokpEnd).configure();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(job.waitForCompletion(true) ? 0 : -1);
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, ParseException {
		AppKuduRedis ak = new AppKuduRedis();
		String path = null;
		String dateday = "";
		String tableName = null;
		String areacode = "";
		if (args.length == 1) {
			path = args[0];
			tableName = DateUtil.getTableName(dateday);
		} else if (args.length == 2) {
			path = args[0];
			tableName = DateUtil.getTableName(args[1]);
		} else if (args.length == 3) {
			path = args[0];
			tableName = DateUtil.getTableName(args[1]);
			areacode = args[2];
		} else {
			System.exit(-1);
		}
		// RedisUtil.setPATH(path);
		ak.init(path);
		// Create the job specification object
		ak.create(path, tableName, dateday, areacode);
	}
}
