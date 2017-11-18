/**  
 * @Title: Radix.java
 * @Package com.util
 * @author houzwhouzw
 * @createdate 2017年9月26日
 * @Description: TODO
 * @function list:
 * @--------------------edit history-----------------
 * @editdate 2017年9月26日
 * @editauthor houzw
 * @Description TODO
 */
package com.util;

/**
 * ClassName: Radix
 * 
 * @author houzw
 * @date 2017年9月26日
 * @Description: TODO
 */
public class Radix {
	/**
	 * 获取数值（10进制）指定位置的值
	 * 
	 * @Description: TODO
	 * @param @param
	 *            num 数字
	 * @param @param
	 *            index 位置
	 * @param @return
	 * @return int
	 * @throws @author
	 *             houzw
	 * @date 2017年9月26日
	 */
	public static int get(long num, int index) {
		return (int) ((num & (0x1 << index)) >> index);
	}
}
