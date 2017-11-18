package com.analysis;

import java.util.Calendar;
import java.util.List;

import com.model.Position;

/**
 * 运行时段分析类
 * 
 * @author hezhaopeng
 * 
 */
public class RuntimeAnalysis {

	/**
	 * 进行分析
	 * 
	 * @param map
	 */
	public static String runtimeAnalysisAndSave(List<Position> list) {

		Double[] num = { 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d };
		long positiontime = 0l;
		int index = 0;
		StringBuffer sbu = new StringBuffer();
		Calendar cal = Calendar.getInstance();
		for (Position vehiclePosition : list) {
			positiontime = vehiclePosition.getPositiontime() * 1000l;
			cal.setTimeInMillis(positiontime);
			index = cal.get(Calendar.HOUR_OF_DAY);
			num[index] = num[index] + vehiclePosition.getDistance();
		}
		for (Double s : num) {
			sbu.append(s > 50 ? "1" : "0");
		}
		return sbu.toString();
	}
}
