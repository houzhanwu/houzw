package com.util;

import java.util.Comparator;

import com.model.Position;

public class ComparatorPositionTime implements Comparator<Position> {

	public int compare(Position obj0, Position obj1) {
		// 比较定位时间
		return (new Long(obj0.getPositiontime())).compareTo(new Long(obj1.getPositiontime()));
	}

}