package com.click2sync.rc.srv.sai.models;

import java.util.HashMap;

import net.iryndin.jdbf.core.DbfRecord;

public class SAIOrderForC2S {
	HashMap<String, DbfRecord> orderlines = new HashMap<String, DbfRecord>();
	DbfRecord rec = null;
	int lineno = 1;
	
	public void addrecord (DbfRecord record) {
		rec = record;
	}
	
	public void addOrderLine(DbfRecord record) {
		orderlines.put(Integer.toString(lineno), record);
		lineno++;
	}
	
	public DbfRecord getRecord() {
		return rec;
	}
	
	public HashMap<String, DbfRecord> getOrderLines() {
		return orderlines;
	}
}
