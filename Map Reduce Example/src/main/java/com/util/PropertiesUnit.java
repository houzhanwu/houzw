package com.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUnit {
	private Properties props = new Properties();

	public PropertiesUnit(String filename) {
		try {
			// InputStream in = this.getClass().getResourceAsStream(filename);
			InputStream in = new FileInputStream(filename);
			props.load(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 根据变量读取配置文件的值
	 * 
	 * @param key
	 *            变量名称
	 * @return String
	 */
	public String readValue(String key) {
		String value = props.getProperty(key);
		return value;
	}

	/**
	 * 写配置文件变量值
	 * 
	 * @param parameterName
	 *            变量名
	 * @param parameterValue
	 *            值
	 */
	public static void writeProperties(String parameterName, String parameterValue) {
		// Properties prop = new Properties();
		// try {
		// InputStream fis = new FileInputStream(url.getFile());
		// prop.load(fis);
		// OutputStream fos = new FileOutputStream(url.getFile());
		// prop.setProperty(parameterName, parameterValue);
		// prop.store(fos, "Update '" + parameterName + "' value");
		// } catch (IOException e) {
		// System.err.println("Visit " + url.getFile() + " for updating "
		// + parameterName + " value error");
		// }
	}
}
