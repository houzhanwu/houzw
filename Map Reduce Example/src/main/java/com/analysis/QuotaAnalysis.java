/**  
 * @Title: QuotaAnalysis.java
 * @Package com.analysis
 * @author houzwhouzw
 * @createdate 2017年7月31日
 * @Description: TODO
 * @function list:
 * @--------------------edit history-----------------
 * @editdate 2017年7月31日
 * @editauthor houzw
 * @Description TODO
 */
package com.analysis;

import java.util.List;

import com.model.Position;

/**
 * ClassName: QuotaAnalysis
 * 
 * @author houzw
 * @date 2017年7月31日
 * @Description: TODO
 */
public class QuotaAnalysis {
	public static String calculation(List<Position> postions) {
		long totalPoint = postions.size();
		long vec1ExceptionPoint = 0;
		long vec2ExceptionPoint = 0;
		long mileExceptionPoint = 0;
		int accesscode = postions.get(0).getAccesscode();
		int trans = postions.get(0).getTrans();

		long oil = 0;
		long electric = 0;

		long gps = 0;
		long compass = 0;
		long glonass = 0;
		long galilel = 0;

		long none = 0;
		long harf = 0;
		long preserve = 0;
		long full = 0;
		for (int i = 0; i < postions.size(); i++) {
			if (i > 0) {
				if (postions.get(i).getVec3() == 0 || (postions.get(i).getDistance() > 0
						&& postions.get(i).getVec3() == postions.get(i - 1).getVec3())) {
					mileExceptionPoint++;
				}

			}
			if (postions.get(i).getVec1() == 0 && postions.get(i).getDistance() > 0) {
				vec1ExceptionPoint++;
			}
			if (postions.get(i).getVec2() == 0 && postions.get(i).getDistance() > 0) {
				vec2ExceptionPoint++;
			}

			// if (Long.toBinaryString(postions.get(i).getState()).length() >
			// 21) {
			switch (get(postions.get(i).getState(), 8) + "" + get(postions.get(i).getState(), 9)) {
			case "00":// 空车
				none++;
				break;
			case "01":// 半载
				harf++;
				break;
			case "10":// 保留
				preserve++;
				break;
			case "11":// 满载
				full++;
				break;
			}
			if (String.valueOf(get(postions.get(i).getState(), 10)).equalsIgnoreCase("1")) {
				oil++;
			}
			if (String.valueOf(get(postions.get(i).getState(), 11)).equalsIgnoreCase("1")) {
				electric++;
			}
			if (String.valueOf(get(postions.get(i).getState(), 18)).equalsIgnoreCase("1")) {
				gps++;
			}
			if (String.valueOf(get(postions.get(i).getState(), 19)).equalsIgnoreCase("1")) {
				compass++;
			}
			if (String.valueOf(get(postions.get(i).getState(), 20)).equalsIgnoreCase("1")) {
				glonass++;
			}
			if (String.valueOf(get(postions.get(i).getState(), 21)).equalsIgnoreCase("1")) {
				galilel++;
			}
		}

		// }
		// return accesscode + "#" + trans + "#" + exceptionPoint + "#" +
		// totalPoint;
		return accesscode + "#" + trans + "_" + vec1ExceptionPoint + "#" + vec2ExceptionPoint + "_" + mileExceptionPoint
				+ "_" + none + "#" + harf + "#" + preserve + "#" + full + "_" + oil + "_" + electric + "_" + gps + "#"
				+ compass + "#" + glonass + "#" + galilel + "_" + totalPoint;

	}

	// public static char getByIndex(long num, int index) {
	// return Long.toBinaryString(num).charAt(index - 1);
	// }
	//
	// public static String getByIndex(long num, int startIndex, int endIndex) {
	// StringBuffer sb = new StringBuffer();
	// for (int i = startIndex; i <= endIndex; i++) {
	// sb.append(Long.toBinaryString(num).charAt(i - 1));
	// }
	// return sb.toString();
	// }

	public static int get(long num, int index) {
		// index --;
		return (int) ((num & (0x1 << index)) >> index);
	}

	public static void main(String[] args) {
		System.out.println(Long.toBinaryString(786691));
		System.out.println(get(786691, 16) + " " + get(786691, 17) + " " + get(786691, 18) + " " + get(786691, 19));
		
	}
}
