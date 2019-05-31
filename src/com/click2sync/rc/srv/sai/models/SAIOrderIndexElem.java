package com.click2sync.rc.srv.sai.models;

public class SAIOrderIndexElem {
	String key = "";
	long lastUpdated = 0;
	int recno = -1;
	
	public SAIOrderIndexElem(String k, long luorder, int rec) {
		super();
		key = k;
		lastUpdated = luorder;
		recno = rec;
	}
	
	public String getKey() {
		return key;
	}
	
	public long getLastUpdated() {
		return lastUpdated;
	}
	
	public int getOrdTableRecNo() {
		return recno;
	}
	
	public boolean equals(SAIOrderIndexElem o) {
		return o.getKey().equals(key);
	}
	
	public void notifyDiscoveredLastUpdate(long lu) {
		if(lu > lastUpdated) {
			lastUpdated = lu;
		}
	}
}
