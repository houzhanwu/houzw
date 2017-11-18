package com.analysis;

import java.util.ArrayList;
import java.util.List;

import com.model.Position;

public class TrackRateAnalysis {

	public static String calculation(List<Position> postions) {
		int length = postions.size();
		int flag = 0;
		int continuousPoint = 0;
		double continuousMileage = 0.0;
		double totalMileage = 0.0;
		double distance;
		List<Double> tempMileages = new ArrayList<Double>();
		for (int i = 0; i < length; i++) {
			if ((i + 1) < length) {
				distance = postions.get(i + 1).getDistance();
				totalMileage += distance;
				if (distance <= 2000) {
					if (tempMileages.size() != 0) {
						for (double tempMileage : tempMileages) {
							continuousMileage += tempMileage;
							continuousPoint++;
						}
						tempMileages.clear();
					}
					continuousPoint++;
					continuousMileage += distance;
					flag = 0;
				} else if (distance > 2000 && distance <= 10000) {
					if (flag <= 5) {
						tempMileages.add(distance);
						flag++;
					} else {
						tempMileages.clear();
					}
				} else {
					if (tempMileages.size() != 0) {
						for (double tempMileage : tempMileages) {
							continuousMileage += tempMileage;
							continuousPoint++;
						}
						tempMileages.clear();
					}
					flag = 0;
				}

			}
		}
		double rate;
		if (continuousMileage == 0) {
			rate = 0;
		} else {
			rate = continuousMileage / totalMileage;
		}
		return rate + "#" + continuousPoint + "#" + continuousMileage;
	}
}
