package com.redis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.analysis.AllopatryAnalysis;
import com.analysis.MileageAnalysis;
import com.analysis.OverSpeedAnalysis;
import com.analysis.PreProcessing;
import com.analysis.RadiusAnalysis;
import com.analysis.RuntimeAnalysis;
import com.analysis.TiredAnalysis;
import com.analysis.TrackRateAnalysis;
import com.google.gson.Gson;
import com.model.Position;
import com.util.ComparatorPositionTime;

public class EarthquakeReducerRedis extends Reducer<Text, Text, Text, Text> {
	private Gson gson = new Gson();
	private List<Position> list;
	private List<Position> listerr;
	private Position position;
	private ComparatorPositionTime comparator = new ComparatorPositionTime();
	private StringBuffer sbu;

	public void reduce(Text key, Iterable<Text> values, Context context) {
		list = new ArrayList<Position>();
		listerr = new ArrayList<Position>();
		int pointlength = 0;
		sbu = new StringBuffer();
		try {
			for (Text value : values) {
				position = gson.fromJson(value.toString(), Position.class);
				if ("00000000000".equals(position.getErrorcode())) {
					list.add(position);
				} else {
					listerr.add(position);
				}
			}
			Collections.sort(list, comparator);
			pointlength = list.size();
			list = PreProcessing.calculation(list);

			sbu.append(pointlength).append("_");
//			sbu.append(ErrorAnalysis.queryErr(listerr)).append("_");
			sbu.append(RuntimeAnalysis.runtimeAnalysisAndSave(list)).append("_");
			sbu.append(pointlength - list.size()).append("_");
			sbu.append(MileageAnalysis.queryMileage(list)).append("_");
			sbu.append(RadiusAnalysis.queryRadius(list)).append("_");
			sbu.append(OverSpeedAnalysis.queryContent(list)).append("_");

			sbu.append(TiredAnalysis.queryContent(list)).append("_");
			sbu.append(TrackRateAnalysis.calculation(list)).append("_");
			sbu.append(AllopatryAnalysis.queryAllopatry(list));
			context.write(key, new Text(sbu.toString()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
