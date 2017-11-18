package com.analysis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.model.Position;
import com.util.Config;

public class AllopatryAnalysis {
	public static final String AREACODE = "110000,120000,130000,140000,150000,210000,220000,230000,310000,320000,330000,340000,350000,360000,370000,410000,420000,430000,440000,450000,460000,500000,510000,520000,530000,540000,610000,620000,630000,640000,650000";

	/**
	 * 异地
	 * 
	 * @param value
	 * @return
	 */
	public static String queryAllopatry(List<Position> value) {
		Map<Integer, Integer> map = new HashMap<Integer, Integer>();
		StringBuffer areacodeNum = new StringBuffer();
		// 遍历list集合
		for (int i = 0; i < value.size(); i++) {
			// 所在地区
			map.put(Config.getAREMAP(String.valueOf(value.get(i).getCuraccesscode() / 10000 * 10000)), 0);
		}
		for (int i = 1; i < 32; i++) {
			if (map.get(i) != null) {
				areacodeNum.append("1");
			} else {
				areacodeNum.append("0");
			}
		}
		return areacodeNum.toString();
	}
}
