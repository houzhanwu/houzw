package com.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class OracleEntityBean implements Writable, DBWritable {
	// VehicleNo
	private String vehicleno = "";
	// PlateColor
	private int platecolor = 0;
	// AccessCode
	private int accesscode = 0;
	// Trans
	private int trans = 0;

	private String info = "";

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		int index = 1;
		statement.setString(index++, this.getVehicleno());
		statement.setInt(index++, this.getPlatecolor());
		statement.setInt(index++, this.getAccesscode());
		statement.setInt(index++, this.getTrans());
		statement.setString(index++, this.getInfo());
		//
		statement.setString(index++, this.getVehicleno());
		statement.setInt(index++, this.getPlatecolor());
		//
		statement.setString(index++, this.getVehicleno());
		statement.setInt(index++, this.getPlatecolor());
		statement.setInt(index++, this.getAccesscode());
		statement.setInt(index++, this.getTrans());
		statement.setString(index++, this.getInfo());
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		this.vehicleno = resultSet.getString(1);
		this.platecolor = resultSet.getInt(2);
		this.accesscode = resultSet.getInt(3);
		this.trans = resultSet.getInt(4);
		this.info = resultSet.getString(5);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.vehicleno);
		out.writeInt(this.platecolor);
		out.writeInt(this.accesscode);
		out.writeInt(this.trans);
		out.writeUTF(this.info);
		//
		out.writeUTF(this.vehicleno);
		out.writeInt(this.platecolor);
		//
		out.writeUTF(this.vehicleno);
		out.writeInt(this.platecolor);
		out.writeInt(this.accesscode);
		out.writeInt(this.trans);
		out.writeUTF(this.info);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

	}

	public String getVehicleno() {
		return vehicleno;
	}

	public void setVehicleno(String vehicleno) {
		this.vehicleno = vehicleno;
	}

	public int getPlatecolor() {
		return platecolor;
	}

	public void setPlatecolor(int platecolor) {
		this.platecolor = platecolor;
	}

	public int getAccesscode() {
		return accesscode;
	}

	public void setAccesscode(int accesscode) {
		this.accesscode = accesscode;
	}

	public int getTrans() {
		return trans;
	}

	public void setTrans(int trans) {
		this.trans = trans;
	}

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

}