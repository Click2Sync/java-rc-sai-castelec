package com.click2sync.rc.srv.sai;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.click2sync.rc.srv.sai.models.SAIOrderIndexElem;
import com.click2sync.rc.srv.sai.models.SAIProdIndexElem;
import com.linuxense.javadbf.DBFWriter;

import net.iryndin.jdbf.core.DbfField;
import net.iryndin.jdbf.core.DbfMetadata;
import net.iryndin.jdbf.reader.DbfReader;

public class SAIProxy {

	static final Pattern VALID_EMAIL_ADDRESS_REGEX = Pattern.compile("^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,6}$",
			Pattern.CASE_INSENSITIVE);
	Main delegate;
	TreeSet<SAIProdIndexElem> indexprod = null;
	Iterator<SAIProdIndexElem> prodindx = null;
	
	TreeSet<SAIOrderIndexElem> indexord = null;
	Iterator<SAIOrderIndexElem> ordindx = null;

	public SAIProxy(Main main) {
		delegate = main;
	}

	private void connect() throws NoSAIException {
		
		Path prodpath = SAIHelper.getProdTableFilePath(delegate);
		Path exispath = SAIHelper.getProdExisTableFilePath(delegate);
		File prodf = new File(prodpath.toString());
		File exisf = new File(exispath.toString());
		if(!prodf.exists() || !exisf.exists()) {
			throw new NoSAIException("Unable to connect to SAI, please check your database settings... "); 
		}

	}

	public void sense() throws NoSAIException {

		ServiceLogger.log("Sensing environment for SAI...");
		connect();

	}

	public void setProductsStreamCursor(Long offset) throws NoSAIException {		
		
		indexprod = SAIHelper.buildProductsTimestampIndex("asc", delegate);
		prodindx = indexprod.iterator();
		ServiceLogger.log("numofproducts: " + indexprod.size() + " offset: " + offset);
		while(prodindx.hasNext()) {
			SAIProdIndexElem elem = prodindx.next();
			if(elem.getLastUpdated() >= offset) {
				break;
			} 
		}

	}

	public boolean hasMoreProducts() throws NoSAIException {
		
		return prodindx.hasNext();

	}

	public JSONObject nextProduct() throws NoSAIException {
		
		SAIProdIndexElem elem = prodindx.next();
		JSONObject prod = SAIHelper.buildC2SProduct(elem, delegate);
		return prod;

	}

	public void setOrdersStreamCursor(Long offset) throws NoSAIException {

		indexord = SAIHelper.buildOrdersTimestampIndex("asc", delegate);
		ordindx = indexord.iterator();
		ServiceLogger.log("numoforders: " + indexord.size() + " offset: " + offset);
		while(ordindx.hasNext()) {
			SAIOrderIndexElem elem = ordindx.next();
			if(elem.getLastUpdated() >= offset) {
				break;
			} 
		}

	}

	public boolean hasMoreOrders() throws NoSAIException {

		return ordindx.hasNext();

	}

	public JSONObject nextOrder() throws NoSAIException {

		SAIOrderIndexElem elem = ordindx.next();
		ServiceLogger.log("NO_MOV: " + elem.getKey() + " LastU: " + elem.getLastUpdated());
		JSONObject ord = SAIHelper.buildC2SOrder(elem, delegate);
		return ord;
	}

	public JSONObject storeProduct(JSONObject product) throws NoSAIException {
		
		boolean isInsert = false;
		if(isInsert) { 
			throw new NoSAIException("SAI is protected, the implementation restricts new products on SAI... "); 
		} else { 
			throw new NoSAIException("SAI is protected, the implementation restricts product updates on SAI... "); 
		}

	}

	public void test() {

		TreeSet<SAIProdIndexElem> prodindex = SAIHelper.buildProductsTimestampIndex("asc", delegate);
		ServiceLogger.log("number of products: " + prodindex.size());
		Iterator<SAIProdIndexElem> it = prodindex.iterator();
		while(it.hasNext()) {
			SAIProdIndexElem next = it.next();
			JSONObject c2sproduct = SAIHelper.buildC2SProduct(next, delegate);
			//System.out.println("SKU:"+next.getKey()+", EXIST: "+next.getExistencia()+", LUPD:"+next.getLastUpdated()+", EXRECNO:"+next.getExisTableRecNo()+", PRRECNO:"+next.getProdTableRecNo());
			//System.out.println("JSON: " + c2sproduct.toJSONString());
		}

	}

	public void testOrders() {

		TreeSet<SAIOrderIndexElem> prodindex = SAIHelper.buildOrdersTimestampIndex("asc", delegate);
		ServiceLogger.log("number of orders: " + prodindex.size());
		Iterator<SAIOrderIndexElem> it = prodindex.iterator();
		while(it.hasNext()) {
			SAIOrderIndexElem next = it.next();
			JSONObject c2sproduct = SAIHelper.buildC2SOrder(next, delegate);
			//System.out.println("SKU:"+next.getKey()+", EXIST: "+next.getExistencia()+", LUPD:"+next.getLastUpdated()+", EXRECNO:"+next.getExisTableRecNo()+", PRRECNO:"+next.getProdTableRecNo());
			System.out.println("JSON: " + c2sproduct.toJSONString());
		}

	}
	
	public void testStoreOrder() throws NoSAIException{
		ServiceLogger.log("entered testStoreOrder()");
		Path orderfile = SAIHelper.getOrdersTableFilePath(delegate);
		String ordertoappendfile = SAIHelper.getOrdersToAppendTableFilePath(delegate);
		JSONParser parser = new JSONParser();
		JSONObject test = null;
		try {
			test = (JSONObject) parser.parse("{\"total\":{\"amount\":450,\"currency\":\"MXN\"},\"dateCreated\":1538520202000,\"orderid\":\"tmp-7aa0790bcf9583e21ce2651b\",\"otherids\":[{\"connectionid\":\"5bd89e925e0868000791a070\",\"id\":\"1823515886\",\"connectiontype\":\"mercadolibre\"}],\"dateClosed\":1538520204000,\"orderItems\":[{\"unitPrice\":450,\"quantity\":1,\"variation_id\":\"\",\"id\":\"8940163300\",\"currencyId\":\"MXN\"},{\"unitPrice\":999,\"quantity\":9,\"variation_id\":\"\",\"id\":\"12345678\",\"currencyId\":\"MXN\"}],\"buyer\":{\"billingaddress\":{\"zipcode\":\"64860\",\"country\":\"Mexico\",\"city\":\"Monterrey\",\"addressline\":\"Benito Juarez 123\",\"state\":\"Nuevo León\"},\"firstName\":\"Test\",\"lastName\":\"Test\",\"phone\":\"(01) 1111-1111\",\"shipmentaddress\":{\"zipcode\":\"64860\",\"country\":\"Mexico\",\"city\":\"Monterrey\",\"addressline\":\"Benito Juarez 123\",\"state\":\"Nuevo León\"},\"id\":223369408,\"email\":\"ttest.2b5n7f+2-oge4demzvge2tsmrt@mail.mercadolibre.com.mx\"},\"status\":\"paid\"}" + 
					"");
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		OutputStream dbfoutput;
		InputStream dbfinput;
		try {
			dbfinput = Files.newInputStream(orderfile);
			DbfReader readerinput = new DbfReader(dbfinput);
			DbfMetadata meta = readerinput.getMetadata();
			meta.setRecordsQty(0);
			Iterator<DbfField> fieldit = meta.getFields().iterator();
			int i = 0;
//		while(fieldit.hasNext()) {				
//			ServiceLogger.log("rowData[" + i + "] = " + "\"\";		//" + fieldit.next().getName());
//			i++;
//		}
			DBFWriter writer = new DBFWriter(new File(ordertoappendfile)); 
			
			JSONArray orderItems = (JSONArray) test.get("orderItems");
		    for (Object o : orderItems) {
		    	Object orderForSAI = SAIHelper.buildOrderRowForBDF(test, (JSONObject) o, delegate);
		    	writer.addRecord((Object[]) orderForSAI);		    	
		    }
			
		    writer.close();
			
			
			
			
		} catch (IOException e) {
			throw new NoSAIException("Problem writing to DBF: " + e.getMessage());
		}
	}
	
	public JSONObject storeOrder(JSONObject order) throws NoSAIException {
		
		Path orderfile = SAIHelper.getOrdersTableFilePath(delegate);
		String ordertoappendfile = SAIHelper.getOrdersToAppendTableFilePath(delegate);
		JSONParser parser = new JSONParser();
		
		OutputStream dbfoutput;
		InputStream dbfinput;
		
		try {
			dbfinput = Files.newInputStream(orderfile);
			DbfReader readerinput = new DbfReader(dbfinput);
			DbfMetadata meta = readerinput.getMetadata();
			meta.setRecordsQty(0);
			Iterator<DbfField> fieldit = meta.getFields().iterator();
			int i = 0;
			DBFWriter writer = new DBFWriter(new File(ordertoappendfile)); 
			
			JSONArray orderItems = (JSONArray) order.get("orderItems");
		    for (Object o : orderItems) {
		    	Object[] orderForSAI = (Object[]) SAIHelper.buildOrderRowForBDF(order, (JSONObject) o, delegate);
		    	order.put("orderid", orderForSAI[0].toString());
		    	order.put("_id", orderForSAI[0].toString());
		    	writer.addRecord(orderForSAI);    	
		    }
			
		    writer.close();
		    return order;
		} catch (IOException e) {
			throw new NoSAIException("Problem writing to DBF: " + e.getMessage());
		}

	}

	static double convertToDouble(Object longValue) {
		double valueTwo = 0; // whatever to state invalid!
		if (longValue instanceof Long) {
			valueTwo = ((Long) longValue).doubleValue();
		} else if (longValue instanceof Float) {
			valueTwo = ((Float) longValue).doubleValue();
		} else if (longValue instanceof Double) {
			valueTwo = ((Double) longValue).doubleValue();
		} else {
			try {
				valueTwo = (double) valueTwo;
			} catch (Exception e) {
				System.out.println("could not find and cast conversion. longValue is instance of " + longValue.getClass());
			}
		}
		return valueTwo;
	}

	public static boolean isValidEmail(String emailStr) {
		Matcher matcher = VALID_EMAIL_ADDRESS_REGEX.matcher(emailStr);
		return matcher.find();
	}

}
