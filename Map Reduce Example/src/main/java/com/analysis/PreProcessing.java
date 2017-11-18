package com.analysis;

import java.util.ArrayList;
import java.util.List;

import com.model.Position;
import com.util.ComputationUtil;

public class PreProcessing {

	public static List<Position> calculation(List<Position> postions) {
		long time;
		double distance;
		double distanceOld = 0.0;
		double speed;

		int length = postions.size();
		int flag = 0;
		if (length == 2) {
			// 计算当前点与上点得距离值
			distance = ComputationUtil.getDistance(postions.get(0).getLon(),
					postions.get(0).getLat(), postions.get(1).getLon(),
					postions.get(1).getLat());
			postions.get(1).setDistance(distance);
		} else {
			int i = 1;
			while (i < length) {
				distance = ComputationUtil.getDistance(
						postions.get(i).getLon(), postions.get(i).getLat(),
						postions.get(i - 1).getLon(), postions.get(i - 1)
								.getLat());
				time = postions.get(i).getPositiontime()
						- postions.get(i - 1).getPositiontime();
				speed = distance / time / 3.6;
				if (speed > 160) {
					distanceOld = distance;
					if (flag == 1) {
						i = i - 1;
						postions.remove(i);
						// 剔除漂移点重新计算 当前角标点位与上个点位得距离差值
						distance = ComputationUtil.getDistance(postions.get(i)
								.getLon(), postions.get(i).getLat(), postions
								.get(i - 1).getLon(), postions.get(i - 1)
								.getLat());
						time = postions.get(i).getPositiontime()
								- postions.get(i - 1).getPositiontime();
						speed = distance / time / 3.6;
						if (speed > 160) {
							distanceOld = distance;
							flag = 1;
						} else {
							postions.get(i).setDistance(distance);
							flag = 0;
						}
					} else {
						flag = 1;
					}
				} else {
					postions.get(i).setDistance(distance);
					if (flag == 1) {
						postions.get(i - 1).setDistance(distanceOld);
					}
					flag = 0;
				}
				i++;
				length = postions.size();
			}
		}
		return postions;
	}

	public static List<Position> exceptInvalidPoint(List<Position> list) {
		List<Position> rlist = new ArrayList<>();
		Position p = null;
		Position lastPositon = list.get(0);
		for (int i = 0; i < list.size(); i++) {
			p = list.get(i);
			if (i > 0) {
				if ((p.getLon() == lastPositon.getLon())
						&& (p.getLat() == lastPositon.getLat())) {
				} else {
					if (p.getLon() != 0 && p.getLat() != 0) {
						rlist.add(p);
						lastPositon = p;
					}
				}
			} else {
				rlist.add(p);
			}
		}
		return rlist;
	}
}
