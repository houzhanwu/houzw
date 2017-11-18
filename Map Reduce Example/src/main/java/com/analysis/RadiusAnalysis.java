package com.analysis;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.model.Position;
import com.util.ComputationUtil;

public class RadiusAnalysis {

	/**
	 * 半径
	 * 
	 * @param value
	 * @return
	 */
	public static String queryRadius(List<Position> value) {
		DecimalFormat df = new DecimalFormat("######0.00");
		// 半径值
		double radius = 0;
		List<Integer> LonList = new ArrayList<Integer>();
		List<Integer> LatList = new ArrayList<Integer>();

		// 遍历list集合
		for (int i = 0; i < value.size(); i++) {
			LonList.add(value.get(i).getLon());
			LatList.add(value.get(i).getLat());
		}
		Collections.sort(LonList);
		Collections.sort(LatList);
		radius = ComputationUtil.getDistance(LonList.get(LonList.size() - 1), LatList.get(LatList.size() - 1), LonList.get(0), LatList.get(0)) / 2;
		return df.format(radius);
	}
}
