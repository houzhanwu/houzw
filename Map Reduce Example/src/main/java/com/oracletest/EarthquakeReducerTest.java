package com.oracletest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
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
import com.model.OracleEntityBean;
import com.model.Position;
import com.util.ComparatorPositionTime;

public class EarthquakeReducerTest extends Reducer<Text, Text, OracleEntityBean, Text> {
	private Gson gson = new Gson();
	private List<Position> list;
	private Position position;
	private OracleEntityBean oeb;
	private ComparatorPositionTime comparator = new ComparatorPositionTime();
	private boolean flag = true;
	private StringBuffer sbu;

	public void reduce(Text key, Iterable<Text> values, Context context) {
		oeb = new OracleEntityBean();
		list = new ArrayList<Position>();
		int pointlength = 0;
		sbu = new StringBuffer();
		try {
			for (Text value : values) {
				position = gson.fromJson(value.toString(), Position.class);
				list.add(position);
				if (flag) {
					BeanUtils.copyProperties(oeb, position);
				}
				flag = false;
			}
			Collections.sort(list, comparator);
			pointlength = list.size();
			list = PreProcessing.calculation(list);
			sbu.append(pointlength).append("_");
			sbu.append(RuntimeAnalysis.runtimeAnalysisAndSave(list)).append("_");
			sbu.append(pointlength - list.size()).append("_");
			sbu.append(MileageAnalysis.queryMileage(list)).append("_");
			sbu.append(RadiusAnalysis.queryRadius(list)).append("_");
			sbu.append(OverSpeedAnalysis.queryContent(list)).append("_");
			sbu.append(TiredAnalysis.queryContent(list)).append("_");
			sbu.append(TrackRateAnalysis.calculation(list)).append("_");
			sbu.append(AllopatryAnalysis.queryAllopatry(list));
			flag = true;
			oeb.setInfo(sbu.toString());
			context.write(oeb, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
