package com.click2sync.rc.srv.sai;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

import javax.imageio.ImageIO;
import javax.xml.bind.DatatypeConverter;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.click2sync.rc.srv.sai.models.SAIOrderForC2S;
import com.click2sync.rc.srv.sai.models.SAIOrderIndexElem;
import com.click2sync.rc.srv.sai.models.SAIProdIndexElem;
import com.click2sync.rc.srv.sai.C2SRCServiceException;
import com.click2sync.rc.srv.sai.ServiceLogger;

import net.iryndin.jdbf.core.DbfMetadata;
import net.iryndin.jdbf.core.DbfRecord;
import net.iryndin.jdbf.reader.DbfReader;

public class SAIHelper {
	
	public static Path getProdTableFilePath(Main delegate) {
		
		return Paths.get(delegate.config.getProperty("productsdb"));
		
	}
	
	public static Path getProdExtTableFilePath(Main delegate) {
		
		return Paths.get(delegate.config.getProperty("productsdbext"));
		
	}
	
	public static Path getTipoAlmacenTableFilePath(Main delegate) {
		
		return Paths.get(delegate.config.getProperty("productsdbtipoalma"));
		
	}
	
	public static Path getImagesTableFilePath(Main delegate) {
		
		return Paths.get(delegate.config.getProperty("productsimgtable"));
		
	}
	
	public static Path getProdExisTableFilePath(Main delegate) {
		
		return Paths.get(delegate.config.getProperty("existenciasdb"));
		
	}
	
	public static Path getOrdersTableFilePath(Main delegate) {
		
		return Paths.get(delegate.config.getProperty("ordersdb"));
		
	}
	
	public static String getOrdersToAppendTableFilePath(Main delegate) {
		
		return delegate.config.getProperty("ordersdb");
		
	}
	
	public static Path getClientTableFilePath(Main delegate) {
		
		return Paths.get(delegate.config.getProperty("clientsdb"));
		
	}
	
	public static Path getMoneyTypeTableFilePath(Main delegate) {
		
		return Paths.get(delegate.config.getProperty("monedasb"));
		
	}
	
	public static SAIProdIndexElem buildSAIProdIndexElem(DbfRecord rec) {

		
		String key = rec.getString("CVE_PROD");
		key = key != null ? key : "";
		int recno = 0;
		SAIProdIndexElem prod;
		
		long t_act_cto = 0;
		long t_act_pre = 0;
		long t_fec_ent = 0;
		long t_fec_ant = 0;
		long t_fvr_mov = 0;
		long prod_lastupdate = 0;
		
		Date f_act_cto;
		try {
			f_act_cto = rec.getDate("F_ACT_CTO");
			t_act_cto = f_act_cto != null ? f_act_cto.getTime() : 0;
			if(t_act_cto > prod_lastupdate) {
				prod_lastupdate = t_act_cto;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		Date f_act_pre;
		try {
			f_act_pre = rec.getDate("F_ACT_PRE");
			t_act_pre = f_act_pre != null ? f_act_pre.getTime() : 0;
			if(t_act_pre > prod_lastupdate) {
				prod_lastupdate = t_act_pre;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		Date f_fec_ent;
		try {
			f_fec_ent = rec.getDate("FEC_ENT");
			t_fec_ent = f_fec_ent != null ? f_fec_ent.getTime() : 0;
			if(t_fec_ent > prod_lastupdate) {
				prod_lastupdate = t_fec_ent;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		Date f_fec_ant;
		try {
			f_fec_ant = rec.getDate("FEC_ANT");
			t_fec_ant = f_fec_ant != null ? f_fec_ant.getTime() : 0;
			if(t_fec_ant > prod_lastupdate) {
				prod_lastupdate = t_fec_ant;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		Date f_fvr_mov;
		try {
			f_fvr_mov = rec.getDate("FVERIMOV");
			t_fvr_mov = f_fvr_mov != null ? f_fvr_mov.getTime() : 0;
			if(t_fvr_mov > prod_lastupdate) {
				prod_lastupdate = t_fvr_mov;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		recno = rec.getRecordNumber();
		
		prod = new SAIProdIndexElem(key, prod_lastupdate, recno);
		
		return prod;
	}
	
	public static SAIOrderIndexElem buildSAIOrderIndexElem(DbfRecord rec) {

		
		String key = rec.getString("NO_MOV");
		key = key != null ? key : "";
		int recno = 0;
		SAIOrderIndexElem ord;
		
		long fechaMov = 0;
		long fechaFac = 0;
		long prod_lastupdate = 0;
		
		Date f_mov;
		try {
			f_mov = rec.getDate("F_MOV");
			fechaMov = f_mov != null ? f_mov.getTime() : 0;
			if(fechaMov > prod_lastupdate) {
				prod_lastupdate = fechaMov;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		Date falta_fac;
		try {
			falta_fac = rec.getDate("FALTA_FAC");
			fechaFac = falta_fac != null ? falta_fac.getTime() : 0;
			if(fechaFac > prod_lastupdate) {
				prod_lastupdate = fechaFac;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		recno = rec.getRecordNumber();
		
		ord = new SAIOrderIndexElem(key, prod_lastupdate, recno);
		
		return ord;
	}

	public static TreeSet<SAIProdIndexElem> buildProductsTimestampIndex(String sortorder, Main delegate) {
		
		Charset stringCharset = Charset.forName("Cp866");
		Path prodfile = SAIHelper.getProdTableFilePath(delegate);
		Path exisfile = SAIHelper.getProdExisTableFilePath(delegate);
		InputStream dbf;
		InputStream dbf2;
		TreeSet<SAIProdIndexElem> prodindex = new TreeSet<SAIProdIndexElem>(new SAIProdLastUpdateComparator(sortorder));
		HashMap<String,SAIProdIndexElem> prodhash = new HashMap<String,SAIProdIndexElem>();
		
		try {
			
			dbf = Files.newInputStream(prodfile);
			DbfRecord rec;
			
			try (DbfReader reader = new DbfReader(dbf)) {
				
				DbfMetadata meta = reader.getMetadata();
				long st = new Date().getTime();
				System.out.println("Read DBF Metadata: " + meta);
				System.out.println("Started at: " + st);
				while ((rec = reader.read()) != null) {
					rec.setStringCharset(stringCharset);
					SAIProdIndexElem prod = SAIHelper.buildSAIProdIndexElem(rec);
					prodhash.put(prod.getKey(), prod);
				}
				long et = new Date().getTime();
				ServiceLogger.log("prodhash size: " + prodhash.size());
				System.out.println("Ended at: " + et);
				System.out.println("Finished on: " + (et - st));
				
				dbf2 = Files.newInputStream(exisfile);
				try (DbfReader reader2 = new DbfReader(dbf2)) {
					meta = reader2.getMetadata();
					st = new Date().getTime();
					System.out.println("Read DBF Metadata: " + meta);
					System.out.println("Started at: " + st);
					while ((rec = reader2.read()) != null) {
						rec.setStringCharset(stringCharset);
						SAIProdIndexElem prod = prodhash.get(rec.getString("CVE_PROD"));
						if(prod != null) {
							long stock_lu = 0;
							try {
								Date f_umod = rec.getDate("FECH_UMOD");
								stock_lu = f_umod != null ? f_umod.getTime() : 0;
							} catch (ParseException e) {
								e.printStackTrace();
							}
							prod.notifyDiscoveredLastUpdate(stock_lu);
							String exisstring = rec.getString("EXISTENCIA");
							double exis = Double.parseDouble(exisstring);
							prod.setExistencias((long)exis);
							prod.setExisTableRecNo(rec.getRecordNumber());
						}
					}
					et = new Date().getTime();
					System.out.println("Ended at: " + et);
					System.out.println("Finished on: " + (et - st));
					Iterator<String> it = prodhash.keySet().iterator();
					while(it.hasNext()) {
						prodindex.add(prodhash.get(it.next()));
					}
					dbf.close();
					
				}catch(IOException e) {
					e.printStackTrace();
					dbf.close();
				}
				
			} catch (IOException e) {
				e.printStackTrace();
				dbf.close();
			}
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		return prodindex;
		
	}
	
	public static TreeSet<SAIOrderIndexElem> buildOrdersTimestampIndex(String sortorder, Main delegate) {
		
		Charset stringCharset = Charset.forName("Cp866");
		Path prodfile = SAIHelper.getOrdersTableFilePath(delegate);
		InputStream dbf;
		TreeSet<SAIOrderIndexElem> ordindex = new TreeSet<SAIOrderIndexElem>(new SAIOrderLastUpdateComparator(sortorder));
		HashMap<String,SAIOrderIndexElem> ordhash = new HashMap<String,SAIOrderIndexElem>();
		String previouskey = "";
		
		try {
			
			dbf = Files.newInputStream(prodfile);
			DbfRecord rec;
			
			try (DbfReader reader = new DbfReader(dbf)) {
				
				long st = new Date().getTime();
				while ((rec = reader.read()) != null) {
					rec.setStringCharset(stringCharset);
					if(rec.getString("TIPO_MOV").equals("S") && !(previouskey.equals(rec.getString("NO_MOV")))) {
						SAIOrderIndexElem ord = SAIHelper.buildSAIOrderIndexElem(rec);
						ordhash.put(ord.getKey(), ord);
					}
					previouskey = rec.getString("NO_MOV");
				}
				long et = new Date().getTime();
				ServiceLogger.log("ordhash size: " + ordhash.size());
				System.out.println("Ended at: " + et);
				System.out.println("Finished on: " + (et - st));
				
				Iterator<String> it = ordhash.keySet().iterator();
				while(it.hasNext()) {
					ordindex.add(ordhash.get(it.next()));
				}
				dbf.close();
				
			} catch (IOException e) {
				e.printStackTrace();
				dbf.close();
			}
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		return ordindex;
		
	}

	public static DbfRecord getRecordFromProds(int recNo, Main delegate) {
		Charset stringCharset = Charset.forName("Cp866");
		Path prodfile = SAIHelper.getProdTableFilePath(delegate);
		InputStream dbf;
		DbfRecord rec = null;
		try {
			dbf = Files.newInputStream(prodfile);
			DbfReader reader = new DbfReader(dbf);
			int i = 1;
			rec = reader.read();
			while (rec != null && i < recNo) {
				rec = reader.read();
				i++;
			}
			rec.setStringCharset(stringCharset);
			reader.close();
			dbf.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return rec;
		
	}
	
	public static DbfRecord getExtendedRecordFromImages(String sku, Main delegate) {
		Charset stringCharset = Charset.forName("Cp866");
		Path prodfile = SAIHelper.getImagesTableFilePath(delegate);
		InputStream dbf;
		DbfRecord rec = null;
		try {
			dbf = Files.newInputStream(prodfile);
			DbfReader reader = new DbfReader(dbf);
			rec = reader.read();
			while (rec != null && !rec.getString("CVE_PROD").equals(sku)) {
				rec = reader.read();
			}
			if(rec != null) {
				if(!rec.getString("CVE_PROD").equals(sku)) {
					rec = null;
				}else {
					rec.setStringCharset(stringCharset);
				}
			}
			reader.close();
			dbf.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return rec;
		
	}
	
	public static DbfRecord getExtendedRecordFromTipoAlmacen(String tial, Main delegate) {
		Charset stringCharset = Charset.forName("Cp866");
		Path prodfile = SAIHelper.getTipoAlmacenTableFilePath(delegate);
		InputStream dbf;
		DbfRecord rec = null;
		try {
			dbf = Files.newInputStream(prodfile);
			DbfReader reader = new DbfReader(dbf);
			rec = reader.read();
			while (rec != null && !rec.getString("CVE_TIAL").equals(tial)) {
				rec = reader.read();
			}
			if(rec != null) {
				if(!rec.getString("CVE_TIAL").equals(tial)) {
					rec = null;
				}else {
					rec.setStringCharset(stringCharset);
				}
			}
			reader.close();
			dbf.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return rec;
		
	}
	
	public static DbfRecord getExtendedRecordFromProds(String sku, Main delegate) {
		Charset stringCharset = Charset.forName("Cp866");
		Path prodfile = SAIHelper.getProdExtTableFilePath(delegate);
		InputStream dbf;
		DbfRecord rec = null;
		try {
			dbf = Files.newInputStream(prodfile);
			DbfReader reader = new DbfReader(dbf);
			rec = reader.read();
			while (rec != null && !rec.getString("CVE_PROD").equals(sku)) {
				rec = reader.read();
			}
			if(rec != null) {
				if(!rec.getString("CVE_PROD").equals(sku)) {
					rec = null;
				}else {
					rec.setStringCharset(stringCharset);
				}
			}
			reader.close();
			dbf.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return rec;
		
	}
	
	public static SAIOrderForC2S getRecordFromOrders(int recNo, Main delegate) {
		Charset stringCharset = Charset.forName("Cp866");
		Path prodfile = SAIHelper.getOrdersTableFilePath(delegate);
		InputStream dbf;
		DbfRecord rec = null;
		SAIOrderForC2S orderforc2s = null;
		try {
			dbf = Files.newInputStream(prodfile);
			DbfReader reader = new DbfReader(dbf);
			int i = 1;
			rec = reader.read();
			while (rec != null && i < recNo) {
				rec = reader.read();
				i++;
			}
			if(rec != null) {
				orderforc2s = new SAIOrderForC2S();
				orderforc2s.addrecord(rec);
				while (rec != null && rec.getString("NO_MOV").equals(orderforc2s.getRecord().getString("NO_MOV"))) {
					orderforc2s.addOrderLine(rec);
					rec.setStringCharset(stringCharset);
					rec = reader.read();
				}
			}
			reader.close();
			dbf.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return orderforc2s;
		
	}
	
	public static DbfRecord getRecordFromClients(String cve_cte, Main delegate) {
		ServiceLogger.log("entered getRecordFromClients()");
		ServiceLogger.log("cve_cte: " + cve_cte);
		Charset stringCharset = Charset.forName("Cp866");
		Path prodfile = SAIHelper.getClientTableFilePath(delegate);
		InputStream dbf;
		DbfRecord rec = null;
		try {
			dbf = Files.newInputStream(prodfile);
			DbfReader reader = new DbfReader(dbf);
			rec = reader.read();
			rec.setStringCharset(stringCharset);
			while (rec != null) {
				if(rec.getString("CVE_CTE").equals(cve_cte)) {
					break;
				} else {
					rec = reader.read();
				}
			}
			reader.close();
			dbf.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return rec;
		
	}
	
	@SuppressWarnings("unchecked")
	public static JSONObject buildC2SProduct(SAIProdIndexElem elem, Main delegate) {
		JSONObject c2sprod = new JSONObject();
		int recNum = elem.getProdTableRecNo();
		DbfRecord prodrec = SAIHelper.getRecordFromProds(recNum, delegate);
		DbfRecord prodtipoalma = SAIHelper.getExtendedRecordFromTipoAlmacen(prodrec.getString("CVE_TIAL"), delegate);
		DbfRecord prodimage = SAIHelper.getExtendedRecordFromImages(prodrec.getString("CVE_PROD"), delegate);
		
		//_id
		c2sprod.put("_id", ""+elem.getKey());
		//sku
		c2sprod.put("sku", ""+elem.getKey());
		//last_updated
		c2sprod.put("last_updated", elem.getLastUpdated());
		//title
		String name = prodrec.getString("NOM_PROD");
		String desc = prodrec.getString("DESC_PROD");
		String clase = prodrec.getString("CSE_PROD");
		String subclase = prodrec.getString("SUB_CSE");
		String unimed = prodrec.getString("UNI_MED");
		String barcode = prodrec.getString("CODBAR");
		String claveprod = prodrec.getString("CVE_PROD");
		String codcurr = prodrec.getString("CVE_MONP");
		String currency = "MXN";
		if(codcurr.equals("1")) {
			currency = "USD";
		}
		String tipoal = "";
		if(prodtipoalma != null) {
			tipoal = prodtipoalma.getString("DES_TIAL");
		}
		
		String title = "";
		if(name != null && name.trim().length() > 0) {
			title += name.trim();
		}
		if(desc != null && desc.trim().length() > 0) {
			if(title.length() > 0) {
				title += " ";
			}
			title += desc.trim();
		}
		String description = title;
		c2sprod.put("title", title);
		//url
		c2sprod.put("url", "");
		//brand
		c2sprod.put("brand", tipoal);
		//mpn
		c2sprod.put("mpn", claveprod);
		//model
		c2sprod.put("model", subclase);
		//description
		c2sprod.put("description", description);
		//variations
		JSONArray variations = new JSONArray();
		c2sprod.put("variations", variations);
		//variation
		JSONObject variation = new JSONObject();
		variations.add(variation);
		//variations.availabilities
		JSONArray availabilities = new JSONArray();
		variation.put("availabilities", availabilities);
		//variations.availabilities.tag
		JSONObject availability = new JSONObject();
		availabilities.add(availability);
		availability.put("tag", "default");
		//variations.availabilities.quantity
		availability.put("quantity", elem.getExistencia());
		//variations.prices
		JSONArray prices = new JSONArray();
		variation.put("prices", prices);
		//variations.prices.tag
		JSONObject price = new JSONObject();
		prices.add(price);
		price.put("tag", "default");
		//variations.prices.currency
		price.put("currency", currency);
		//variations.prices.number
		price.put("number", prodrec.getBigDecimal("PREC_PROD").floatValue());
		//variations.images.url
		JSONArray images = new JSONArray();
		variation.put("images", images);
		
		if(prodimage != null) {
			
			String picurl = ""+prodimage.getString("IMAGEN");
			if(picurl.length()>0) {
				
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				String imgpath = delegate.config.getProperty("picturesfolder")+picurl;
				String imgurl = "";
				try {
					BufferedImage image = ImageIO.read(new File(imgpath));
					if(imgpath.indexOf(".GIF")>-1) {
						ImageIO.write(image, "GIF", baos);
					}else {
						ImageIO.write(image, "PNG", baos);
					}
				} catch (IOException e) {
				    ServiceLogger.error(new C2SRCServiceException("Could not read supposed image from SAP ("+imgpath+"). "+e.getMessage()));
				}
				String bytesdrawed = DatatypeConverter.printBase64Binary(baos.toByteArray());
				if(bytesdrawed.length()>0) {
					if(imgpath.indexOf(".GIF")>-1) {
						imgurl = "data:image/gif;base64," + bytesdrawed;
					}else {
						imgurl = "data:image/png;base64," + bytesdrawed;
					}
					JSONObject image = new JSONObject();
					images.add(image);
					image.put("url", imgurl);
				}
				
			}
			
		}
		
		//variations.videos.url
		JSONArray videos = new JSONArray();
		variation.put("videos", videos);
		//variations.barcode
		variation.put("barcode", barcode);
		//variations.size
		variation.put("size", clase);
		//variations.color
		variation.put("color", unimed);
		
		return c2sprod;
		
	}
	
	@SuppressWarnings("unchecked")
	public static JSONObject buildC2SOrder(SAIOrderIndexElem elem, Main delegate) {
		JSONObject c2sord = null;
		JSONObject total = new JSONObject();
		int recNum = elem.getOrdTableRecNo();
		SAIOrderForC2S saiorder = getRecordFromOrders(recNum, delegate);
		if(saiorder != null) {
			c2sord = new JSONObject();
			String clientcode = saiorder.getRecord().getString("CVE_CTE");
			//buyer
			if (clientcode.equals("0")) {
				//buyer empty
				JSONObject buyerInfo = new JSONObject();
				buyerInfo.put("id", "");
				buyerInfo.put("email", "");
				buyerInfo.put("phone", "");
				buyerInfo.put("firstName", "");
				buyerInfo.put("lastName", "");
				JSONObject billingAddress = new JSONObject();
				billingAddress.put("addressline", "");
				billingAddress.put("zipcode", "");
				billingAddress.put("city", "");
				billingAddress.put("state", "");
				billingAddress.put("country", "");
				JSONObject shipmentAddress = new JSONObject();
				shipmentAddress.put("addressline", "");
				shipmentAddress.put("zipcode", "");
				shipmentAddress.put("city", "");
				shipmentAddress.put("state", "");
				shipmentAddress.put("country", "");
				buyerInfo.put("billingaddress", billingAddress);
				buyerInfo.put("shipmentaddress", shipmentAddress);
				c2sord.put("buyer", buyerInfo);
			} else {
				DbfRecord clientrec = getRecordFromClients(clientcode, delegate);
				if(clientrec != null) {
					//buyer
					JSONObject buyerInfo = new JSONObject();
					buyerInfo.put("id", clientrec.getString("CVE_CTE"));
					buyerInfo.put("email", clientrec.getString("EMAIL_CTE"));
					buyerInfo.put("phone", clientrec.getString("TEL1_CTE"));
					buyerInfo.put("firstName", clientrec.getString("NOM_CTE"));
					buyerInfo.put("lastName", "");
					//billing address
					JSONObject billingAddress = new JSONObject();
					String streetbilling = clientrec.getString("DIR_CTE") != null  ? clientrec.getString("DIR_CTE") : "";
					String colbilling = clientrec.getString("COL_CTE") != null  ? clientrec.getString("COL_CTE") : "";
					String addcompletebill = null;
					if (streetbilling.equals("")) {
						addcompletebill = "";
					} else if (colbilling.equals("") ) {
						addcompletebill = streetbilling;
					} else {
						addcompletebill = streetbilling + " " + colbilling;
					}
					billingAddress.put("addressline", addcompletebill);
					billingAddress.put("zipcode", clientrec.getString("CP_CTE"));
					billingAddress.put("city", clientrec.getString("CD_CTE"));
					billingAddress.put("state", clientrec.getString("EDO_CTE"));
					billingAddress.put("country", "");
					//shipment address
					JSONObject shipmentAddress = new JSONObject();
					String streetshipment = clientrec.getString("DIR_ENT") != null ? clientrec.getString("DIR_ENT") : "";
					String colshipment = clientrec.getString("COL_ENT") != null ? clientrec.getString("COL_ENT") : "";
					String addcompleteship = null;
					if (streetshipment.equals("")) {
						addcompleteship = "";
					} else if (colshipment.equals("") ) {
						addcompleteship = streetshipment;
					} else {
						addcompleteship = streetshipment + " " + colshipment;
					}
					shipmentAddress.put("addressline", addcompleteship);
					shipmentAddress.put("zipcode", clientrec.getString("CP_ENT"));
					shipmentAddress.put("city", clientrec.getString("CD_ENT"));
					shipmentAddress.put("state", clientrec.getString("EDO_ENT"));
					shipmentAddress.put("country", "");
					buyerInfo.put("billingaddress", billingAddress);
					buyerInfo.put("shipmentaddress", shipmentAddress);
					c2sord.put("buyer", buyerInfo);
				}else {
					JSONObject buyerInfo = new JSONObject();
					buyerInfo.put("id", "");
					buyerInfo.put("email", "");
					buyerInfo.put("phone", "");
					buyerInfo.put("firstName", "");
					buyerInfo.put("lastName", "");
					JSONObject billingAddress = new JSONObject();
					billingAddress.put("addressline", "");
					billingAddress.put("zipcode", "");
					billingAddress.put("city", "");
					billingAddress.put("state", "");
					billingAddress.put("country", "");
					JSONObject shipmentAddress = new JSONObject();
					shipmentAddress.put("addressline", "");
					shipmentAddress.put("zipcode", "");
					shipmentAddress.put("city", "");
					shipmentAddress.put("state", "");
					shipmentAddress.put("country", "");
					buyerInfo.put("billingaddress", billingAddress);
					buyerInfo.put("shipmentaddress", shipmentAddress);
					c2sord.put("buyer", buyerInfo);
				}
			}
			JSONArray orderItems = new JSONArray();
			
			//_id
			c2sord.put("_id", ""+elem.getKey());
			//orderid
			c2sord.put("orderid", ""+elem.getKey());
			//last_updated
			c2sord.put("last_updated", elem.getLastUpdated());
			//status
			c2sord.put("status", "");
			//dateCreated
			c2sord.put("dateCreated", "");
			//dateClosed
			c2sord.put("dateClosed", "");
			
			//orderItems
			//Get line items
			double sumPrice = 0;
			Iterator<String> it = saiorder.getOrderLines().keySet().iterator();
			while(it.hasNext()) {
				DbfRecord recordline = saiorder.getOrderLines().get(it.next());
				JSONObject ordLine = new JSONObject();
				int cantidadproducto = Math.round(Float.parseFloat(recordline.getString("CANT_PROD")));
				ordLine.put("id", recordline.getString("CVE_PROD"));
				ordLine.put("variation_id", "");
				ordLine.put("quantity", cantidadproducto);
				ordLine.put("unitPrice", recordline.getString("PRECI_PROD"));
				ordLine.put("currencyId", "MXN");
				orderItems.add(ordLine);
				//little mult to calculate the price of order
				double preciProd = Double.parseDouble(recordline.getString("PRECI_PROD"));
				sumPrice = sumPrice + (cantidadproducto * preciProd);
			}
			c2sord.put("orderItems", orderItems);
				
			//total
			total.put("amount", sumPrice);
			total.put("currency", "MXN");
			c2sord.put("total", total);
			ServiceLogger.log("total: " + total.toString());
		}else {
			
		}
		
		return c2sord;
		
	}
	
	public static Object buildOrderRowForBDF (JSONObject c2sOrder, JSONObject lineorder, Main delegate) throws IOException {
		
		Charset stringCharset = Charset.forName("Cp866");
		Path orderfile = SAIHelper.getOrdersTableFilePath(delegate);
		Path currencyfile = SAIHelper.getMoneyTypeTableFilePath(delegate);
		InputStream dbfOrders;
		InputStream dbfCurrency;
		DbfRecord orderRec = null;
		DbfRecord currencyRec = null;
        Object orderRowData[] = new Object[89];
		String previouskey = "";
		String newOrderItemId = "";
		float newOrderItemQuantity = (float) 0;
		float newOrderUnitPrice = (float) 0;
		String orderCurrency = "";

		dbfOrders = Files.newInputStream(orderfile);
		DbfReader orderReader = new DbfReader(dbfOrders);
		orderRec = orderReader.read();
		orderRec.setStringCharset(stringCharset);
		while ((orderRec = orderReader.read()) != null) {
			previouskey = orderRec.getString("NO_MOV");
		}
		orderReader.close();
		int newOrderKey = Integer.parseInt(previouskey) + 1;
		long orderDate = (long) c2sOrder.get("dateCreated");
		String changeOrderDate = new java.text.SimpleDateFormat("dd/MM/yyyy").format(new java.util.Date(orderDate));
        @SuppressWarnings("deprecation")
		Date newOrderDate = new Date(changeOrderDate);
		String newOrderHour = new java.text.SimpleDateFormat("HH:mm:ss").format(new java.util.Date(orderDate));
		String newOrderMonth = new java.text.SimpleDateFormat("MM").format(new java.util.Date(orderDate));
		String newOrderYear = new java.text.SimpleDateFormat("yyyy").format(new java.util.Date(orderDate));
		
		dbfCurrency = Files.newInputStream(currencyfile);
		DbfReader currencyReader = new DbfReader(dbfCurrency);

		int newPartida = 1;
		
		JSONObject jsonLineItem = (JSONObject) lineorder;
        newOrderItemId = (String) jsonLineItem.get("id");
        newOrderItemQuantity = (long) jsonLineItem.get("quantity");
        newOrderUnitPrice = (long) jsonLineItem.get("unitPrice");
        orderCurrency = (String) jsonLineItem.get("currencyId");
        String currencyInTable = "";
        String currencyTypeInTable = "";
        
		while ((currencyRec = currencyReader.read()) != null) {
			currencyRec.setStringCharset(stringCharset);
			if(currencyRec.getString("MONSAT").equals(orderCurrency)) {
				currencyInTable = currencyRec.getString("CVE_MON");
			} 
			if(currencyRec.getString("MONSAT").equals("USD")) {
				currencyTypeInTable = currencyRec.getString("TIP_CAM");
			}
		}
		
		currencyReader.close();

        Object rowData[] = new Object[89];
	    rowData[0] = newOrderKey;          //NO_MOV
	    rowData[1] = newOrderDate;          //F_MOV
	    rowData[2] = newOrderItemId;          //CVE_PROD
	    rowData[3] = null;         //REF_MOV
	    rowData[4] = null;          //NO_REF
	    rowData[5] = newOrderItemQuantity;          //CANT_PROD
	    rowData[6] = null;          //COSTO_ENT
	    rowData[7] = null;          //CTO_PROD
	    rowData[8] = newOrderUnitPrice;          //PRECI_PROD
	    rowData[9] = "S";          //TIPO_MOV
	    rowData[10] = 0;         //MEDPROD
	    rowData[11] = "GENERAL";         //ORIG_MOV
	    rowData[12] = null;         //DEST_MOV
	    rowData[13] = 0;         //CVE_PROV
	    rowData[14] = 0;         //CVE_AGE
	    rowData[15] = 0;         //CVE_CTE
	    rowData[16] = "GENERAL";         //LUGAR
	    rowData[17] = "1";         //CVE_MOV
	    rowData[18] = null;         //OBS
	    rowData[19] = newOrderHour;         //HORA_MOV
	    rowData[20] = Integer.parseInt(delegate.config.getProperty("saiuserfororders"));         //USUARIO
	    rowData[21] = Integer.parseInt(currencyInTable);         //CVE_MON
	    rowData[22] = Double.parseDouble(currencyTypeInTable);         //TIP_CAM
	    rowData[23] = "MAT";         //CVE_SUC
	    rowData[24] = newOrderMonth;         //MES
	    rowData[25] = newOrderYear;         //A
	    rowData[26] = 1;         //TRANS
	    rowData[27] = null;         //LOTE
	    rowData[28] = 0;         //USAALM
	    rowData[29] = null;         //CIERRE
	    rowData[30] = 0;         //CVE_CEN
	    rowData[31] = 1;         //PRIORIDAD
	    rowData[32] = 0;         //NO_PEDC
	    rowData[33] = null;         //SUC_ENT
	    rowData[34] = 0;         //NO_REQ
	    rowData[35] = 0;         //NUMPOL
	    rowData[36] = newPartida;         //PARTIDA
	    rowData[37] = 0;         //PARORDCOM
	    rowData[38] = 0;         //DIFTOLERAN
	    rowData[39] = 0;         //MOVCAN
	    rowData[40] = null;         //SUCCAN
	    rowData[41] = null;         //REF_LOTE
	    rowData[42] = 0;         //CTO_PRODT
	    rowData[43] = null;         //CVE_PRODK
	    rowData[44] = 0;         //PART_KIT
	    rowData[45] = null;         //UNIUSU
	    rowData[46] = 0;         //FACUSU
	    rowData[47] = 0;         //CANUSU
	    rowData[48] = 0;         //CENUSU
	    rowData[49] = 0;         //CPRUSU
	    rowData[50] = 0;         //DESC1
	    rowData[51] = 0;         //DESC2
	    rowData[52] = 0;         //CTOENTCOM
	    rowData[53] = 0;         //DTOENTCOM
	    rowData[54] = null;         //ORDREFL
	    rowData[55] = null;         //TURNO
	    rowData[56] = null;         //DATOEST3
	    rowData[57] = 0;         //SALDO
	    rowData[58] = 0;         //NO_REM
	    rowData[59] = null;         //SUC_REM
	    rowData[60] = null;         //LUGARINTER
	    rowData[61] = null;         //NEW_MED
	    rowData[62] = null;         //CVE_COMP
	    rowData[63] = 0;         //GASTOTRADO
	    rowData[64] = 0;         //SALDUEPEPS
	    rowData[65] = 0;         //PARTESPE
	    rowData[66] = null;         //SINCOMPRA
	    rowData[67] = 0;         //T_PRORR
	    rowData[68] = 0;         //PRORR
	    rowData[69] = null;         //FALTA_FAC
	    rowData[70] = 0;         //FORPARIC
	    rowData[71] = null;         //FECH_LOTE
	    rowData[72] = 0;         //FOLIO_SRV
	    rowData[73] = null;         //SUC_SRV
	    rowData[74] = 0;         //IEPSEC
	    rowData[75] = 0;         //IVAEC
	    rowData[76] = 0;         //RETISREC
	    rowData[77] = 0;         //RETIVAEC
	    rowData[78] = null;         //CALIVAEC
	    rowData[79] = 0;         //ID_UNICO
	    rowData[80] = 0;         //PMOVS
	    rowData[81] = 0;         //FACTORIEPS
	    rowData[82] = 0;         //IVA_IEPS
	    rowData[83] = 0;         //PORCENIEPS
	    rowData[84] = 0;         //IEPS_PROD
	    rowData[85] = 0;         //FOL_STRAN
	    rowData[86] = null;         //SUC_STRAN
	    rowData[87] = 0;         //PARTSTRAN
	    rowData[88] = null;         //NMRETOT
	    
	    orderRowData = rowData;
	    newPartida ++;
		
		dbfOrders.close();
		dbfCurrency.close();
	
		return orderRowData;
	}
	
}

class SAIProdLastUpdateComparator implements Comparator<SAIProdIndexElem>{
	
	String sortorder = "asc";
	
	SAIProdLastUpdateComparator(String so){
		if(so.equals("desc")) {
			sortorder = so;
		}
	}

	@Override
	public int compare(SAIProdIndexElem arg0, SAIProdIndexElem arg1) {
		if(sortorder.equals("asc")) {
			if(arg0.getLastUpdated() > arg1.getLastUpdated()) {
				return 1;
			}else if(arg0.getLastUpdated() < arg1.getLastUpdated()) {
				return -1;
			}else if(arg0.getLastUpdated() == arg1.getLastUpdated()) {
				return -1;
			} else {
				return 0;
			}
		}else {
			if(arg0.getLastUpdated() > arg1.getLastUpdated()) {
				return -1;
			}else if(arg0.getLastUpdated() < arg1.getLastUpdated()) {
				return 1;
			}else if(arg0.getLastUpdated() == arg1.getLastUpdated()) {
				return 1;
			} else {
				return 0;
			}
		}
	}
	
}

class SAIOrderLastUpdateComparator implements Comparator<SAIOrderIndexElem>{
	
	String sortorder = "asc";
	
	SAIOrderLastUpdateComparator(String so){
		if(so.equals("desc")) {
			sortorder = so;
		}
	}

	@Override
	public int compare(SAIOrderIndexElem arg0, SAIOrderIndexElem arg1) {
		if(sortorder.equals("asc")) {
			if(arg0.getLastUpdated() > arg1.getLastUpdated()) {
				return 1;
			}else if(arg0.getLastUpdated() < arg1.getLastUpdated()) {
				return -1;
			}else if(arg0.getLastUpdated() == arg1.getLastUpdated()) {
				return -1;
			} else {
				return 0;
			}
		}else {
			if(arg0.getLastUpdated() > arg1.getLastUpdated()) {
				return -1;
			}else if(arg0.getLastUpdated() < arg1.getLastUpdated()) {
				return 1;
			}else if(arg0.getLastUpdated() == arg1.getLastUpdated()) {
				return 1;
			} else {
				return 0;
			}
		}
	}
	
}