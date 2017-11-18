package com.model;

import java.io.Serializable;

public class OracleEntity implements Serializable {

	private static final long serialVersionUID = 48362047645801717L;

	private String vehicleno = "";
	// PlateColor
	private int platecolor = 0;
	// AccessCode
	private int accesscode = 0;

	// Trans
	private int trans = 0;

	private String info = "";

	public int getTrans() {
		return trans;
	}

	public void setTrans(int trans) {
		this.trans = trans;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
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

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

}
