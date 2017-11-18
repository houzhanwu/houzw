package com.util;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateUtil {

	static Logger logger = LoggerFactory.getLogger(DateUtil.class);
	private static final SimpleDateFormat NORM_DATETIME_FORMAT = new SimpleDateFormat("yyyyMMdd");
	private static final SimpleDateFormat NORM_DATEMONTH_FORMAT = new SimpleDateFormat("yyyyMM");
	private static String[] tableTime = null;
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
			"yyyy" + File.separator + "MM" + File.separator + "dd" + File.separator + "HH");

	public static long reduce(String datetime1, String datetime2) {
		Date d1 = parse(datetime1, "yyyy-MM-dd HH:mm:ss");
		Date d2 = parse(datetime2, "yyyy-MM-dd HH:mm:ss");
		long time1 = d1.getTime();
		long time2 = d2.getTime();
		return time1 - time2;
	}

	public static Date parse(String dateString, String format) {
		try {
			return new SimpleDateFormat(format).parse(dateString);
		} catch (ParseException e) {
			logger.error("Parse " + dateString + " with format " + format + " error!", e);
		}
		return null;
	}

	public static String getDay() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		Date date = cal.getTime();
		return NORM_DATETIME_FORMAT.format(date);
	}

	public static String dateToStr(Date date) {

		return NORM_DATETIME_FORMAT.format(date);
	}

	public static String dateToStr(Date date, String formart) {
		return new SimpleDateFormat(formart).format(date);
	}

	public static String getTableName(String dateday) throws ParseException {
		Date date = new Date();
		Calendar cal = Calendar.getInstance();
		if ("".equals(dateday)) {
			cal.add(Calendar.DATE, -1);
			date = cal.getTime();
		} else {
			date = NORM_DATETIME_FORMAT.parse(dateday);
			cal.setTime(date);
		}
		StringBuffer sb = new StringBuffer();
		sb.append("impala::position.CTTIC_VehiclePosition_");
		// 先把字符串转成Date类型
		String tableName = NORM_DATEMONTH_FORMAT.format(date);
		sb.append(tableName).append("_");
		int day = cal.get(Calendar.DATE);
		if (day <= 8) {
			sb.append(1);
		} else if (day <= 16 && day >= 9) {
			sb.append(2);
		} else if (day <= 24 && day >= 17) {
			sb.append(3);
		} else {
			sb.append(4);
		}
		// sb.append("_test");
		return sb.toString();
	}

	public static long getStartTime(String dateday) throws ParseException {
		Date date = null;
		Calendar cal = Calendar.getInstance();
		if ("".equals(dateday)) {
			cal.add(Calendar.DATE, -1);
			date = cal.getTime();
		} else {
			date = NORM_DATETIME_FORMAT.parse(dateday);
			cal.setTime(date);
		}

		Date datedaystart = null;
		try {
			datedaystart = NORM_DATETIME_FORMAT.parse(NORM_DATETIME_FORMAT.format(date));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		// 获取毫秒数
		long longDate = datedaystart.getTime() / 1000;
		return longDate;
	}

	public static long getEndTime(String dateday) throws ParseException {
		Date date = null;
		Calendar cal = Calendar.getInstance();
		if ("".equals(dateday)) {
			date = cal.getTime();
		} else {
			date = NORM_DATETIME_FORMAT.parse(dateday);
			cal.setTime(date);
			cal.add(Calendar.DATE, 1);
			date = cal.getTime();
		}
		Date datedayend = null;
		try {
			datedayend = NORM_DATETIME_FORMAT.parse(NORM_DATETIME_FORMAT.format(date));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 获取毫秒数
		long longDate = datedayend.getTime() / 1000;
		return longDate;
	}

	/**
	 * 判断某一时间是否在一个区间内
	 * 
	 * @param sourceTime
	 *            时间区间,半闭合,如[10:00-20:00)
	 * @param curTime
	 *            需要判断的时间 如10:00
	 * @return
	 * @throws IllegalArgumentException
	 */
	public boolean isInTime(String sourceTime, String curTime) {
		if (sourceTime == null || !sourceTime.contains("-") || !sourceTime.contains(":")) {
			throw new IllegalArgumentException("Illegal Argument arg:" + sourceTime);
		}
		if (curTime == null || !curTime.contains(":")) {
			throw new IllegalArgumentException("Illegal Argument arg:" + curTime);
		}
		String[] args = sourceTime.split("-");
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
		try {
			long now = sdf.parse(curTime).getTime();
			long start = sdf.parse(args[0]).getTime();
			long end = sdf.parse(args[1]).getTime();
			if (args[1].equals("00:00")) {
				args[1] = "24:00";
			}
			if (end < start) {
				if (now >= end && now < start) {
					return false;
				} else {
					return true;
				}
			} else {
				if (now >= start && now < end) {
					return true;
				} else {
					return false;
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Illegal Argument arg:" + sourceTime);
		}

	}

	public static void main(String[] args) throws ParseException {
		System.out.println(DateUtil.getTableName("20170701"));

		System.out.println(DateUtil.getStartTime("20170701"));
		System.out.println(DateUtil.getEndTime("20170701"));

	}

	public static String[] getTableTime() {
		if (tableTime == null) {
			tableTime = new String[3];
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DATE, -1);
			tableTime[0] = String.valueOf(cal.get(Calendar.YEAR));
			tableTime[1] = cal.get(Calendar.MONTH) + 1 < 10 ? "0" + (cal.get(Calendar.MONTH) + 1)
					: String.valueOf(cal.get(Calendar.MONTH) + 1);
			tableTime[2] = cal.get(Calendar.DATE) < 10 ? "0" + cal.get(Calendar.DATE)
					: String.valueOf(cal.get(Calendar.DATE));
		}
		return tableTime;
	}

	public static String getFileName() {
		return DATE_FORMAT.format(new Date());
	}

}
