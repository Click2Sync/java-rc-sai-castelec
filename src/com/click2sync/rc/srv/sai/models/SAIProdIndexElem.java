package com.click2sync.rc.srv.sai.models;

public class SAIProdIndexElem {
	
	String key = "";
	long lastUpdated = 0;
	int recno_prodTable = -1;
	int recno_exisTable = -1;
	long exist = 0;
	
	public SAIProdIndexElem(String k, long luprice, int rec) {
		super();
		key = k;
		lastUpdated = luprice;
		recno_prodTable = rec;
	}
	
	public String getKey() {
		return key;
	}
	
	public long getLastUpdated() {
		return lastUpdated;
	}
	
	public void setExisTableRecNo(int rec) {
		recno_exisTable = rec;
	}
	
	public void setExistencias(long e) {
		exist = e;
	}
	
	public int getExisTableRecNo() {
		return recno_exisTable;
	}
	
	public long getExistencia() {
		return exist;
	}
	
	public int getProdTableRecNo() {
		return recno_prodTable;
	}
	
	public boolean equals(SAIProdIndexElem o) {
		return o.getKey().equals(key);
	}
	
	public void notifyDiscoveredLastUpdate(long lu) {
		if(lu > lastUpdated) {
			lastUpdated = lu;
		}
	}

}
