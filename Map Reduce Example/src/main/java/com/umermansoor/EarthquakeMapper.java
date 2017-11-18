package com.umermansoor;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.kudu.client.RowResult;

import com.google.gson.Gson;
import com.model.Position;

/**
 * 
 * @ClassName: EarthquakeMapper
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author songwei
 * @date 2017年6月19日 下午6:13:33
 * 
 */
public class EarthquakeMapper extends
		Mapper<NullWritable, RowResult, Text, Text> {
	private Gson gson = new Gson();

	public void map(NullWritable key, RowResult value, Context context)
			throws IOException, InterruptedException {

		StringBuffer sb = new StringBuffer();
		sb.append(value.getString("vehicleno")).append("_")
				.append(value.getInt("platecolor"));
		Position position = new Position();
		position.setVehicleno(value.getString("vehicleno"));
		position.setPlatecolor(value.getInt("platecolor"));
		position.setAccesscode(value.getInt("accesscode"));
		position.setAlarm(value.getInt("alarm"));
		// position.setAltitude(value.getInt("altitude"));
		position.setCity(value.getInt("city"));
		position.setCuraccesscode(value.getInt("curaccesscode"));
		// position.setDirection(value.getInt("direction"));
		// position.setEncrypt(value.getInt("encrypt"));
		position.setLat(value.getInt("lat"));
		position.setLon(value.getInt("lon"));
		position.setPositiontime(value.getLong("positiontime"));
		// position.setReserved(value.getString("reserved"));
		// position.setRoadcode(value.getInt("roadcode"));
		position.setState(value.getInt("state"));
		position.setTrans(value.getInt("trans"));
//		position.setErrorcode(value.getInt("errorcode"));
		// position.setUpdatetime(value.getLong("updatetime"));
		// position.setVec1(value.getInt("vec1"));
		// position.setVec2(value.getInt("vec2"));
		// position.setVec3(value.getInt("vec3"));
		// Emit that the row is defined
		context.write(new Text(sb.toString()), new Text(gson.toJson(position)));
	}
}
