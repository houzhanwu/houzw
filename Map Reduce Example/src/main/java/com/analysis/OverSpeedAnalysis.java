package com.analysis;

import java.util.List;

import com.model.Position;

/**
 * 离线分析-超速分析类
 * 
 * @author houzw
 * @ClassName : OverSpeed
 * @Version 版本
 * @ModifiedBy 修改人
 * @Copyright 公司名称
 * @date 2017年5月31日 上午10:55:55
 */
public class OverSpeedAnalysis {
	public static final String s1 = "#";// 分隔符

	public static String queryContent(List<Position> value) {
		long duration = 0; // 时长s
		long sDuration = 0; // 超速时长
		long sPoint = 0; // 超速点数
		long sFrequency = 0; // 超速次数
		double sMile = 0; // 超速里程
		String sday = ""; // 超速时长#时长#超速点数#超速次数#超速里程
		boolean sAlarm = false; // 本次是否超速报警
		boolean sAlarmold = false; // 上次是否超速报警

		long stime = 0; // 单次超速时长
		double smile = 0; // 单次超速里程
		int spt = 0; // 单次超速点数
		double mile_one = 0;
		long time_one = 0;
		Position position = null;
		Position prePosition = null;
		for (int i = 0; i < value.size(); i++) {
			if (i >= 1) {
				position = (Position) value.get(i);
				prePosition = (Position) value.get(i - 1);
				sAlarm = (position.getAlarm() & 2) == 2;
				sAlarmold = (prePosition.getAlarm() & 2) == 2;
				mile_one = position.getDistance();
				time_one = position.getPositiontime() - prePosition.getPositiontime();
				duration += time_one;

				if (sAlarm) {
					spt++;
					if (sAlarmold) {
						stime += time_one;
						smile += mile_one;
					}
				} else {
					if (sAlarmold) {
						stime += time_one;
						smile += mile_one;
						if (spt == 1 && time_one > 30) {
							sDuration += time_one;
							sMile += mile_one;
							sPoint += spt;
							if (time_one % 60 > 0) {
								sFrequency += time_one / 60 + 1;
							} else {
								sFrequency += time_one / 60;
							}
						} else if (stime > 30) {
							sDuration += stime;
							sMile += smile;
							sPoint += spt;
							if (stime % 60 > 0) {
								sFrequency += stime / 60 + 1;
							} else {
								sFrequency += stime / 60;
							}
						}
						stime = 0;
						smile = 0;
						spt = 0;
					}
				}
			}
		}
		sday = sDuration + s1 + duration + s1 + sPoint + s1 + sFrequency + s1 + new java.text.DecimalFormat("######0.00").format(sMile);
		return sday;
	}

}
