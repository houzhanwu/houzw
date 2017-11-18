package com.model;

import java.io.Serializable;

public class Point implements Serializable {
	private static final long serialVersionUID = 7296548966321411217L;
	private double lon;
	private double lat;

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	@Override
	public String toString() {
		return "Point [lon=" + lon + ", lat=" + lat + "]";
	}

}
