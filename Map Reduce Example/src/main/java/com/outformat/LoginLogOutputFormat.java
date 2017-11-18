package com.outformat;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import redis.clients.jedis.Jedis;

import com.util.DateUtil;
import com.util.RedisUtil;

public class LoginLogOutputFormat<K extends DBWritable, V> extends OutputFormat<K, V> {
	/**
	 * 重点也是定制一个RecordWriter类，每一条reduce处理后的记录，我们便可将该记录输出到数据库中
	 */
	protected static class RedisRecordWriter<K, V> extends RecordWriter<K, V> {
		private Jedis jedis; // redis的client实例

		public RedisRecordWriter(Jedis jedis) {
			this.jedis = jedis;
		}

		@Override
		public void write(K key, V value) throws IOException, InterruptedException {

			boolean nullKey = key == null;
			boolean nullValue = value == null;
			if (nullKey || nullValue)
				return;
			if (jedis == null) {
				jedis = RedisUtil.getJedis();
			}

			jedis.hset(key.toString(), DateUtil.getDay(), value.toString());
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			if (jedis != null)
				RedisUtil.returnResource(jedis); // 关闭链接
		}
	}

	public static void setOutputPath(Job job) {
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		Jedis jedis = RedisUtil.getJedis();
		// 构建一个redis，这里你可以自己根据实际情况来构建数据库连接对象
		// System.out.println("构建RedisRecordWriter");
		return new RedisRecordWriter<K, V>(jedis);
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
	}

}