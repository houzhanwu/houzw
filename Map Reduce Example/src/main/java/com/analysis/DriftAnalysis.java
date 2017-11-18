package com.analysis;

import java.util.List;

import com.model.Position;

public class DriftAnalysis {

	public static String calculation(List<Position> postions) {
		int DriftPointNum = 0;
		for (int i = 0; i < postions.size(); i++) {
			if (postions.get(i).isDrift()) {
				DriftPointNum++;
			}
		}
		return String.valueOf(DriftPointNum);
	}

}
