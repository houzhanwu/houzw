package com.util;

import java.io.IOException;
import java.io.InputStream;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public final class RedisUtil {

	// Redis服务器IP
	private static String ADDR = "172.16.111.6";

	// Redis的端口号
	private static int PORT = 6379;

	// 可用连接实例的最大数目，默认值为8；
	// 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
	private static int MAX_ACTIVE = 1024;

	// 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
	private static int MAX_IDLE = 200;

	// 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
	private static int MAX_WAIT = 10000;

	private static int TIMEOUT = 10000;

	// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
	private static boolean TEST_ON_BORROW = true;

	private static JedisPool jedisPool = null;

	static {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(MAX_ACTIVE);
		config.setMaxIdle(MAX_IDLE);
		config.setMaxWaitMillis(MAX_WAIT);
		config.setTestOnBorrow(TEST_ON_BORROW);
		jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT);
	}

	/**
	 * 初始化Redis连接池
	 */
	public static void setPATH(String pATH) {
		try {
			// init(pATH);
			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxIdle(MAX_ACTIVE);
			config.setMaxIdle(MAX_IDLE);
			config.setMaxWaitMillis(MAX_WAIT);
			config.setTestOnBorrow(TEST_ON_BORROW);
			jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void init(String path) {
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
		ADDR = pu.readValue("ADDR");
		PORT = Integer.parseInt(pu.readValue("PORT"));
		MAX_ACTIVE = Integer.parseInt(pu.readValue("MAX_ACTIVE"));
		MAX_IDLE = Integer.parseInt(pu.readValue("MAX_IDLE"));
		MAX_WAIT = Integer.parseInt(pu.readValue("MAX_WAIT"));
		TIMEOUT = Integer.parseInt(pu.readValue("TIMEOUT"));
		TEST_ON_BORROW = Boolean.parseBoolean(pu.readValue("TEST_ON_BORROW"));
	}

	/**
	 * 获取Jedis实例
	 * 
	 * @return
	 */
	public synchronized static Jedis getJedis() {
		try {
			if (jedisPool != null) {
				Jedis resource = jedisPool.getResource();
				return resource;
			} else {
				return null;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 释放jedis资源
	 * 
	 * @param jedis
	 */
	@SuppressWarnings("deprecation")
	public static void returnResource(final Jedis jedis) {
		if (jedis != null) {
			jedisPool.returnResource(jedis);
		}
	}
}