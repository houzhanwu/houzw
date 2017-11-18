package com;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONObject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import com.analysis.AllopatryAnalysis;
import com.analysis.MileageAnalysis;
import com.analysis.OverSpeedAnalysis;
import com.analysis.PreProcessing;
import com.analysis.RadiusAnalysis;
import com.analysis.RuntimeAnalysis;
import com.analysis.TiredAnalysis;
import com.analysis.TrackRateAnalysis;
import com.model.Position;

public class test {
	public static void main(String[] args) throws IOException {
		List<Position> list = new ArrayList<Position>();
		File file = new File("C://work/test.txt");
		LineIterator it = FileUtils.lineIterator(file, "UTF-8");
		try {
			while (it.hasNext()) {
				String line = it.nextLine();
				if (!"".equals(line)) {
					JSONObject jsonobject = JSONObject.fromObject(line);
					Position position = (Position) JSONObject.toBean(jsonobject, Position.class);
					list.add(position);
				}
				if (list.size() == 617) {
					break;
				}
			}
		} finally {
			LineIterator.closeQuietly(it);
		}
		int pointlength = list.size();
		list = PreProcessing.calculation(list);
		System.out.println(RuntimeAnalysis.runtimeAnalysisAndSave(list));
		System.out.println(pointlength - list.size());
		System.out.println(MileageAnalysis.queryMileage(list));
		System.out.println(RadiusAnalysis.queryRadius(list));
		System.out.println(OverSpeedAnalysis.queryContent(list));
		System.out.println(TiredAnalysis.queryContent(list));
		System.out.println(TrackRateAnalysis.calculation(list));
		System.out.println(AllopatryAnalysis.queryAllopatry(list));
		System.out.println("共" + list.size() + "个点");
	}
}
