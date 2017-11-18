package com.util;

public class ConvertUtil {

	/**
	 * 省编码转省名称(全称)
	 * 
	 * @param code
	 * @return
	 */
	public static String CodeToProvinces(String code) {
		String ret = "其他";
		if (code.equals("110000")) {
			ret = "北京";
		} else if (code.equals("120000")) {
			ret = "天津";
		} else if (code.equals("130000")) {
			ret = "河北";
		} else if (code.equals("140000")) {
			ret = "山西";
		} else if (code.equals("150000")) {
			ret = "内蒙古";
		} else if (code.equals("210000")) {
			ret = "辽宁";
		} else if (code.equals("220000")) {
			ret = "吉林";
		} else if (code.equals("230000")) {
			ret = "黑龙江";
		} else if (code.equals("310000")) {
			ret = "上海";
		} else if (code.equals("320000")) {
			ret = "江苏";
		} else if (code.equals("330000")) {
			ret = "浙江";
		} else if (code.equals("340000")) {
			ret = "安徽";
		} else if (code.equals("350000")) {
			ret = "福建";
		} else if (code.equals("360000")) {
			ret = "江西";
		} else if (code.equals("370000")) {
			ret = "山东";
		} else if (code.equals("410000")) {
			ret = "河南";
		} else if (code.equals("420000")) {
			ret = "湖北";
		} else if (code.equals("430000")) {
			ret = "湖南";
		} else if (code.equals("440000")) {
			ret = "广东";
		} else if (code.equals("450000")) {
			ret = "广西";
		} else if (code.equals("460000")) {
			ret = "海南";
		} else if (code.equals("500000")) {
			ret = "重庆";
		} else if (code.equals("510000")) {
			ret = "四川";
		} else if (code.equals("520000")) {
			ret = "贵州";
		} else if (code.equals("530000")) {
			ret = "云南";
		} else if (code.equals("540000")) {
			ret = "西藏";
		} else if (code.equals("610000")) {
			ret = "陕西";
		} else if (code.equals("620000")) {
			ret = "甘肃";
		} else if (code.equals("630000")) {
			ret = "青海";
		} else if (code.equals("640000")) {
			ret = "宁夏";
		} else if (code.equals("650000")) {
			ret = "新疆";
		}
		return ret;
	}

	/**
	 * 输入一个省份名称返回省份编码
	 * 
	 * @param inputStr
	 *            省份名称
	 * @return
	 */
	public static String ProvincesToCode(String inputStr) {
		if (("北京").equals(inputStr))
			return "110000";
		if (("天津").equals(inputStr))
			return "120000";
		if (("河北").equals(inputStr))
			return "130000";
		if (("山西").equals(inputStr))
			return "140000";
		if (("内蒙古").equals(inputStr))
			return "150000";
		if (("辽宁").equals(inputStr))
			return "210000";
		if (("吉林").equals(inputStr))
			return "220000";
		if (("黑龙江").equals(inputStr))
			return "230000";
		if (("上海").equals(inputStr))
			return "310000";
		if (("江苏").equals(inputStr))
			return "320000";
		if (("浙江").equals(inputStr))
			return "330000";
		if (("安徽").equals(inputStr))
			return "340000";
		if (("福建").equals(inputStr))
			return "350000";
		if (("江西").equals(inputStr))
			return "360000";
		if (("山东").equals(inputStr))
			return "370000";
		if (("河南").equals(inputStr))
			return "410000";
		if (("湖北").equals(inputStr))
			return "420000";
		if (("湖南").equals(inputStr))
			return "430000";
		if (("广东").equals(inputStr))
			return "440000";
		if (("广西").equals(inputStr))
			return "450000";
		if (("海南").equals(inputStr))
			return "460000";
		if (("重庆").equals(inputStr))
			return "500000";
		if (("四川").equals(inputStr))
			return "510000";
		if (("贵州").equals(inputStr))
			return "520000";
		if (("云南").equals(inputStr))
			return "530000";
		if (("西藏").equals(inputStr))
			return "540000";
		if (("陕西").equals(inputStr))
			return "610000";
		if (("甘肃").equals(inputStr))
			return "620000";
		if (("青海").equals(inputStr))
			return "630000";
		if (("宁夏").equals(inputStr))
			return "640000";
		if (("新疆").equals(inputStr))
			return "650000";
		if (("台湾").equals(inputStr))
			return "710000";
		if (("香港").equals(inputStr))
			return "720000";
		if (("澳门").equals(inputStr))
			return "730000";
		return "";
	}

	/**
	 * 输入一个颜色代码返回颜色名称
	 * 
	 * @param inputStr应答结果代码
	 * @returnix
	 */
	public static String TransColorCode(String inputStr) {
		if (("1").equals(inputStr))
			return "蓝色";
		if (("2").equals(inputStr))
			return "黄色";
		if (("3").equals(inputStr))
			return "黑色";
		if (("4").equals(inputStr))
			return "白色";
		if (("8").equals(inputStr))
			return "补充色";
		if (("9").equals(inputStr))
			return "其他";
		return "";
	}

	/**
	 * 输入一个颜色代码返回颜色名称
	 * 
	 * @param inputStr应答结果代码
	 * @returnix
	 */
	public static String HBcode(String inputStr) {

		if (("鄂A").equals(inputStr))
			return "武汉市";
		if (("鄂B").equals(inputStr))
			return "黄石市";
		if (("鄂C").equals(inputStr))
			return "十堰市";
		if (("鄂D").equals(inputStr))
			return "荆州市";
		if (("鄂E").equals(inputStr))
			return "宜昌市";
		if (("鄂F").equals(inputStr))
			return "襄阳市";
		if (("鄂G").equals(inputStr))
			return "鄂州市";
		if (("鄂H").equals(inputStr))
			return "荆门市";
		if (("鄂J").equals(inputStr))
			return "黄冈市";
		if (("鄂K").equals(inputStr))
			return "孝感市";
		if (("鄂L").equals(inputStr))
			return "咸宁市";
		if (("鄂M").equals(inputStr))
			return "仙桃市";
		if (("鄂N").equals(inputStr))
			return "潜江市";
		if (("鄂P").equals(inputStr))
			return "神农架";
		if (("鄂Q").equals(inputStr))
			return "恩施州";
		if (("鄂R").equals(inputStr))
			return "天门市";
		if (("鄂S").equals(inputStr))
			return "随州市";
		return "其它";
	}

	/**
	 * 输入一个颜色名称返回颜色代码
	 * 
	 * @param inputStr应答结果代码
	 * @returnix
	 */
	public static String CodetoColor(String inputStr) {
		if (("蓝色").equals(inputStr))
			return "1";
		if (("黄色").equals(inputStr))
			return "2";
		if (("黑色").equals(inputStr))
			return "3";
		if (("白色").equals(inputStr))
			return "4";
		if (("补充色").equals(inputStr))
			return "8";
		if (("其他").equals(inputStr))
			return "9";
		return "";
	}

	/**
	 * 返回行业名称; exp: 011->班线客车
	 */
	public static String transName(String trans) {
		String ret = "其它";
		if (trans.equals("011") || trans.equals("11")) {
			ret = "班线客车";
		} else if (trans.equals("012") || trans.equals("12")) {
			ret = "旅游包车";
		} else if (trans.equals("030") || trans.equals("30")) {
			ret = "危险品运输车";
		}
		return ret;
	}

	public static String bissinessName(String trans) {
		String ret = "其它";
		trans = trans.substring(0, 3);

		if (trans.equals("010") || trans.equals("011")) {
			ret = "班线客车";
		} else if (trans.equals("012") || trans.equals("013") || trans.equals("014")) {
			ret = "旅游包车";
		} else if (trans.equals("030") || trans.equals("031") || trans.equals("032")) {
			ret = "危险品运输车";
		}
		return ret;

	}

	/**
	 * 返回行业名称011，012：客运车；030：货运车。
	 * 
	 * @param trans
	 * @return
	 */
	public static String trantoname(String trans) {
		String ret = "其他";
		if (trans.equals("011") || trans.equals("11")) {
			ret = "客运车";
		} else if (trans.equals("012") || trans.equals("12")) {
			ret = "客运车";
		} else if (trans.equals("030") || trans.equals("30")) {
			ret = "货运车";
		} else if ("011,012".equals(trans)) {
			ret = "客运车";
		}

		return ret;
	}

	/**
	 * 
	 * 返回行业编码011,012,030
	 * 
	 * @param trans（经营范围编码）
	 * @return
	 */
	public static String btrantotran(String trans) {
		String ret = "011";
		if (trans.contains("011") || trans.contains("010")) {
			ret = "011";
		}
		if (trans.contains("012") || trans.contains("013") || trans.contains("014")) {
			ret = "012";
		}
		if (trans.contains("030") || trans.contains("031") || trans.contains("032")) {
			ret = "030";
		}
		return ret;
	}

	/**
	 * 
	 * 11 班线客车 12 旅游包车 30 危险品运输车 20 普货车辆 999 其它车辆
	 * 
	 * @param trans（经营范围编码）
	 * @return
	 */
	public static String bTranToName(String trans) {
		String ret = "其他车辆";
		if (trans.contains("11")) {
			ret = "班线客车";
		}
		if (trans.contains("12")) {
			ret = "旅游包车";
		}
		if (trans.contains("20")) {
			ret = "重载货车";
		}
		if (trans.contains("30")) {
			ret = "危险品运输";
		}
//		if (trans.contains("999")) {
//			ret = "其他车辆";
//		}
		return ret;
	}

	/**
	 * 返回行业编码; exp: 班线客车->011
	 */
	public static String nameTrans(String trans) {
		String ret = "其他";
		if (trans.equals("班线客车")) {
			ret = "11";
		} else if (trans.equals("旅游包车")) {
			ret = "12";
		} else if (trans.equals("危险品运输车")) {
			ret = "30";
		}
		return ret;
	}
}
