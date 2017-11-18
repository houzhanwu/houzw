//package com.analysis;
//
//import java.util.List;
//
//import com.model.Position;
//
//public class ErrorAnalysis {
//
//	/**
//	 * 错误数据
//	 * 
//	 * @param value
//	 * @return
//	 */
//	public static String queryErr(List<Position> list) {
//		Long[] num = { 0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l };
//		StringBuffer errinfo = new StringBuffer();
//		errinfo.append(list.size());
//		for (int g = 0; g < list.size(); g++) {
//			for (int i = 0; i < list.get(g).getErrorcode().length(); i++) {
//				if ("1".equals(list.get(g).getErrorcode().substring(i, i - 1))) {
//					num[i] = num[i] + 1;
//				}
//			}
//		}
//		for (int i = 0; i < num.length; i++) {
//			errinfo.append("#").append(num[i]);
//		}
//		return errinfo.toString();
//	}
//}
