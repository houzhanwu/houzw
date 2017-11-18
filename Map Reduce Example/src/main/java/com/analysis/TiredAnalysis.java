package com.analysis;

import java.util.List;

import com.model.Position;

/**
 * 离线分析-疲劳分析类
 * 
 * @author houzw
 * @ClassName : OverSpeed
 * @Version 版本
 * @ModifiedBy 修改人
 * @Copyright 公司名称
 * @date 2017年5月31日 上午10:55:56
 */
public class TiredAnalysis {
	private static final String s1 = "#";// 分隔符

	public static String queryContent(List<Position> value) {
		long tDuration = 0; // 疲劳时长
		long tPoint = 0; // 疲劳点数
		long tFrequency = 0;// 疲劳次数
		double tMile = 0.00; // 疲劳里程
		String tday = ""; // 疲劳时长#疲劳点数#疲劳次数#疲劳里程
							// 2#-12545#0#1#16532#9335105.10#163465209689.90
		boolean tAlarm = false; // 本次是否疲劳报警
		boolean tAlarmold = false; // 上次是否疲劳报警

		double mile_one = 0;
		long time_one = 0;
		Position position = null;
		Position prePosition = null;
		int tpt = 0;
		for (int i = 0; i < value.size(); i++) {
			if (i >= 1) {
				position = (Position) value.get(i);
				prePosition = (Position) value.get(i - 1);

				tAlarm = (position.getAlarm() & 4) == 4;
				tAlarmold = (prePosition.getAlarm() & 4) == 4;
				mile_one = position.getDistance();
				time_one = position.getPositiontime() - prePosition.getPositiontime();

				if (tAlarm) {
					tpt++;
					if (tAlarmold) {
						tDuration += time_one;
						tMile += mile_one;
					}
				} else {
					if (tAlarmold) {
						tDuration += time_one;
						tMile += mile_one;
						tPoint += tpt;
						tFrequency++;
						tpt = 0;
					}
				}
			}
		}
		tday = tDuration + s1 + tPoint + s1 + tFrequency + s1 + new java.text.DecimalFormat("######0.00").format(tMile);
		return tday;
	}

}
