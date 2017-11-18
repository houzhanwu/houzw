package com.analysis;

import java.text.DecimalFormat;
import java.util.List;

import com.model.Position;

public class MileageAnalysis {

	/**
	 * 里程
	 * 
	 * @param value
	 * @return
	 */
	public static String queryMileage(List<Position> value) {
		DecimalFormat df = new DecimalFormat("######0.00");
		double mileage = 0;
		for (int i = 0; i < value.size(); i++) {
			mileage += value.get(i).getDistance();
		}
		// 最终结果lastPosition
		return df.format(mileage);
	}
}
