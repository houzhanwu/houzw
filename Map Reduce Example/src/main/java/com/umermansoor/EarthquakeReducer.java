package com.umermansoor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.analysis.PreProcessing;
import com.google.gson.Gson;
import com.model.Position;
import com.util.ComparatorPositionTime;
import com.util.ComputationUtil;

public class EarthquakeReducer extends Reducer<Text, Text, Text, Text> {

	private Gson gson = new Gson();
	private List<Position> list;
	private Position position;
	private Position lastPosition;
	private ComparatorPositionTime comparator = new ComparatorPositionTime();
	private StringBuffer sbu;
	double distance = 0;
	public static String separator = System.getProperty("line.separator");
	int count = 0;

	public List<List<Position>> result = null;

	public void reduce(Text key, Iterable<Text> values, Context context) {
		list = new ArrayList<Position>();
		result = new ArrayList<List<Position>>();
		sbu = new StringBuffer();
		try {

			if (new Random().nextInt(10000) < 1000) {// 0——99 0 1 2
				for (Text value : values) {
					position = gson.fromJson(value.toString(), Position.class);
//					if (position.getErrorcode() == 0) {
//						list.add(position);
//					}
				}
			}

			Collections.sort(list, comparator);// 定位时间排序
			list = PreProcessing.calculation(list);// 过滤漂移点
			list = PreProcessing.exceptInvalidPoint(list);// 除去（0，0）点和重复点

			// 获取指定行业类别的

			// list = getByTransType(list, 30);

			if (list.size() > 500) {
				getDataString(list);
				formatResult(result);
				context.write(new Text(" "), new Text(sbu.toString()));
			}
		} catch (

		Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 获取指定行业类别的数据
	 * 
	 * @param datalist
	 * @param transType
	 *            行业类别
	 * @return
	 */
	@SuppressWarnings("unused")
	private List<Position> getByTransType(List<Position> datalist, int transType) {
		List<Position> dlist = new ArrayList<Position>();
		Position positon = null;
		for (int i = 0; i < datalist.size(); i++) {
			positon = datalist.get(i);
			if (positon.getTrans() == transType) {
				dlist.add(positon);
			}
		}
		return datalist;
	}

	private void formatResult(List<List<Position>> result) {
		List<Position> rlist = null;
		for (int i = 0; i < result.size(); i++) {
			rlist = result.get(i);
			if (rlist != null && rlist.size() > 100) {
				for (int j = 0; j < rlist.size(); j++) {
					position = rlist.get(j);
					lastPosition = rlist.get(0);
					if (j > 0) {
						lastPosition = rlist.get(j - 1);
					}
					if (j == 0) {
						sbu.append(position.getVehicleno() + "_" + position.getPlatecolor() + "#["
								+ position.getLon() / 100d + "," + position.getLat() / 100d + ",");
					} else if (j == rlist.size() - 1) {
						sbu = new StringBuffer(sbu.substring(0, sbu.length() - 1) + "],");
					} else {
						// sbu.append(new BigDecimal(position.getLon() / 100d -
						// lastPosition.getLon() / 100d)
						// .setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue()
						// + ","
						// + new BigDecimal(position.getLat() / 100d -
						// lastPosition.getLat() / 100d)
						// .setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue()
						// + ",");

						sbu.append(position.getLon() / 100d + "," + position.getLat() / 100d + ",");
					}
				}
			}
		}

	}

	private void getDataString(List<Position> list) {
		FIRST: while (true) {
			OK: for (int i = 0; i < list.size(); i++) {
				position = list.get(i);
				lastPosition = list.get(0);
				if (i > 0) {
					lastPosition = list.get(i - 1);
				}
				// 两点间距离>160的点将集合拆分
				distance = ComputationUtil.getDistance(position.getLon(), position.getLat(), lastPosition.getLon(),
						lastPosition.getLat());
				if (i == list.size() - 1) {
					result.add(list);
					break FIRST;
				}
				if (distance > 5000) {// 去除两点距离大于5公里的点
					if (i >= 100) {
						result.add(new ArrayList<Position>(list.subList(0, i - 1)));
					}
					if (list.size() - i - 2 > 100) {
						list = new ArrayList<Position>(list.subList(i + 1, list.size() - 1));
						break OK;

					} else {
						break FIRST;
					}

				}

			}
		}

	}

}
