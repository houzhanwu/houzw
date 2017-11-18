package com.util;

import java.util.HashMap;

public class Config {
	private static HashMap<String, String> VINMAP = new HashMap<String, String>();
	public static HashMap<String, Integer> AREMAP = new HashMap<String, Integer>();
	static {
		VINMAP.put("110000", "京A,京a");
		VINMAP.put("120000", "津A,津a");
		VINMAP.put("130000", "冀A,冀a");
		VINMAP.put("140000", "晋A,晋a");
		VINMAP.put("150000", "蒙A,蒙a");
		VINMAP.put("210000", "辽A,辽a");
		VINMAP.put("220000", "吉A,吉a");
		VINMAP.put("230000", "黑A,黑a");
		VINMAP.put("310000", "沪A,沪a");
		VINMAP.put("320000", "苏A,苏a");
		VINMAP.put("330000", "浙A,浙a");
		VINMAP.put("340000", "皖A,皖a");
		VINMAP.put("350000", "闽A,闽a");
		VINMAP.put("360000", "赣A,赣a");
		VINMAP.put("370000", "鲁A,鲁a");
		VINMAP.put("410000", "豫A,豫a");
		VINMAP.put("420000", "鄂A,鄂a");
		VINMAP.put("430000", "湘A,湘a");
		VINMAP.put("440000", "粤A,粤a");
		VINMAP.put("450000", "桂A,桂a");
		VINMAP.put("460000", "琼A,琼a");
		VINMAP.put("500000", "渝A,渝a");
		VINMAP.put("510000", "川A,川a");
		VINMAP.put("520000", "黔A,黔a");
		VINMAP.put("530000", "滇A,滇a");
		VINMAP.put("540000", "藏A,藏a");
		VINMAP.put("610000", "陕A,陕a");
		VINMAP.put("620000", "甘A,甘a");
		VINMAP.put("630000", "青A,青a");
		VINMAP.put("640000", "宁A,宁a");
		VINMAP.put("650000", "新A,新a");

		AREMAP.put("110000", 1);
		AREMAP.put("120000", 2);
		AREMAP.put("130000", 3);
		AREMAP.put("140000", 4);
		AREMAP.put("150000", 5);
		AREMAP.put("210000", 6);
		AREMAP.put("220000", 7);
		AREMAP.put("230000", 8);
		AREMAP.put("310000", 9);
		AREMAP.put("320000", 10);
		AREMAP.put("330000", 11);
		AREMAP.put("340000", 12);
		AREMAP.put("350000", 13);
		AREMAP.put("360000", 14);
		AREMAP.put("370000", 15);
		AREMAP.put("410000", 16);
		AREMAP.put("420000", 17);
		AREMAP.put("430000", 18);
		AREMAP.put("440000", 19);
		AREMAP.put("450000", 20);
		AREMAP.put("460000", 21);
		AREMAP.put("500000", 22);
		AREMAP.put("510000", 23);
		AREMAP.put("520000", 24);
		AREMAP.put("530000", 25);
		AREMAP.put("540000", 26);
		AREMAP.put("610000", 27);
		AREMAP.put("620000", 28);
		AREMAP.put("630000", 29);
		AREMAP.put("640000", 30);
		AREMAP.put("650000", 31);
	}

	public static String getVINMAP(String code) {
		return VINMAP.get(code);
	}

	public static Integer getAREMAP(String i) {
		return AREMAP.get(i);
	}
}
