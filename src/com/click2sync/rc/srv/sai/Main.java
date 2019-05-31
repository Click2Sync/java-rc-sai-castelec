package com.click2sync.rc.srv.sai;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

import org.json.simple.JSONObject;

public class Main {
	
	final String configFile = "./config.properties";
	Properties config;
	SAIProxy sai;
	C2SProxy c2s;
	Long longpauseMillis = 10000L;
	Long normalpauseMillis = 1000L;
	static Main app;
	
	Main(){
		
		c2s = new C2SProxy(this);
		sai = new SAIProxy(this);
		config = new Properties();
		InputStream input;
		try {
			input = new FileInputStream(configFile);
			config.load(input);
			longpauseMillis = Long.parseLong(config.getProperty("longpausemillis"));
			normalpauseMillis = Long.parseLong(config.getProperty("normalpausemillis"));
		} catch (NumberFormatException e) {
			ServiceLogger.error(new C2SRCServiceException("Could not read pause time settings from configuration: "+e.getMessage()));
		} catch (FileNotFoundException e) {
			ServiceLogger.error(new C2SRCServiceException("Configuration file not found: "+e.getMessage()));
		} catch (IOException e) {
			ServiceLogger.error(new C2SRCServiceException("Could not access configuration file: "+e.getMessage()));
		}
		
	}
	
	public void loop() throws InterruptedException {
		
		try {
			work();
			pauseWork(true);
		} catch (C2SUnreachableException e) {
			ServiceLogger.error(e);
			pauseWork(true);
		} catch (NoSAIException e) {
			ServiceLogger.error(e);
			pauseWork(true);
		} catch (C2SRCServiceException e) {
			ServiceLogger.error(e);
			pauseWork(true);
		}
		
	}
	
	public void pauseWork(boolean longPause) throws InterruptedException {
		
		if(longPause) {
			Thread.sleep(longpauseMillis);
		} else {
			Thread.sleep(normalpauseMillis);
		}
		
	}
	
	public void senseEnvironmentForSAIServices() throws NoSAIException {
		
		sai.sense();
		
	}
	
	public void senseEnvironmentForC2SReachability() throws C2SUnreachableException {
		
		c2s.sense();
		
	}
	
	public static void abstractLoop() {
		
		ServiceLogger.log("loop");
		if(app == null) {
			app = new Main();
			Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));
			try {
				app.senseEnvironmentForSAIServices();
				app.senseEnvironmentForC2SReachability();
			} catch (NoSAIException e) {
				ServiceLogger.error(e);
				try {
					app.pauseWork(true);
					app = null;
				} catch (InterruptedException e1) {
					ServiceLogger.error(e1);
				}
				return;
			} catch (C2SUnreachableException e) {
				ServiceLogger.error(e);
				try {
					app.pauseWork(true);
					app = null;
				} catch (InterruptedException e1) {
					ServiceLogger.error(e1);
				}
				return;
			}
		}
		try {
			app.loop();
		} catch (InterruptedException e) {
			ServiceLogger.error(e);
		}
		
	}

	public static void main(String[] args) {
		
		while(true) {
			try {
				abstractLoop();
//				app = new Main();
//				app.test();
			}catch(Exception e) {
				ServiceLogger.error(e);
			}
		}

	}
	
	@SuppressWarnings("unused")
	private void test() {
		
		try {
			sai.sense();
//			sai.testOrders();
			sai.testStoreOrder();
		} catch (NoSAIException e) {
			e.printStackTrace();
		}
		
	}
	
	private boolean checkIfWeAreFetchingRemote() throws C2SUnreachableException, C2SRCServiceException {
		
		return c2s.checkIfFetchingRemote();
	}
	
	private boolean checkIfWeArePullingToRemote() throws C2SUnreachableException, C2SRCServiceException {
		
		return c2s.checkIfPullingToRemote();
	}
	
	private void fetchRemote() throws C2SUnreachableException,NoSAIException, InterruptedException {
		
		boolean uploadProducts = true;
		boolean uploadOrders = true;
		ServiceLogger.log("Fetching remote...");
		String strategy = c2s.getStrategy();
		String upstreamstatus = c2s.getUpstreamStatus();
		String entity = c2s.getEntity();
		Long offset = c2s.getCursorOffset();
		
		if(upstreamstatus != null && upstreamstatus.equals("waiting")) {
			//if is waiting, good, change to initialized
			c2s.setInitializeUpload(strategy);
			upstreamstatus = c2s.getUpstreamStatus();
			//(if webonline responds error, then notify of strange behavior to server)
		}
		if(upstreamstatus != null && upstreamstatus.equals("initialized")) {
			//if is initialized, check cursor
			//from cursor start upload
			//when packet uploaded, renew offsets
			if(entity != null && entity.equals("products")) {
				if(uploadProducts) {
					if(strategy != null && strategy.equals("pingsample")) {
						sai.setProductsStreamCursor(offset);
						if(sai.hasMoreProducts()) {
							JSONObject product = sai.nextProduct();
							c2s.setProductToUploadOnBuffer(product, strategy);
						}
					} else {
						sai.setProductsStreamCursor(offset);
						while(sai.hasMoreProducts()) {
							JSONObject product = sai.nextProduct();
							c2s.setProductToUploadOnBuffer(product, strategy);
						}
					}
				}
				c2s.setFinishProductUpload(strategy);
				offset = c2s.getCursorOffset();
				entity = c2s.getEntity();
			}
			if(entity != null && entity.equals("orders")) {
				if(uploadOrders) {
					if(strategy != null && strategy.equals("pingsample")) {
						sai.setOrdersStreamCursor(offset);
						if(sai.hasMoreOrders()) {
							JSONObject order = sai.nextOrder();
							c2s.setOrderToUploadOnBuffer(order, strategy);
						}
					} else {
						sai.setOrdersStreamCursor(offset);
						while(sai.hasMoreOrders()) {
							JSONObject order = sai.nextOrder();
							if(order != null) {
								c2s.setOrderToUploadOnBuffer(order, strategy);
							}else {
								continue;
							}
						}
					}
				}
				c2s.setFinishOrderUpload(strategy);
				offset = c2s.getCursorOffset();
			}
			//when no more in SAI, send finish command
			c2s.setFinishUpload(strategy);
			upstreamstatus = c2s.getUpstreamStatus();
		}
		if(upstreamstatus != null && (upstreamstatus.equals("finished") || upstreamstatus.equals("finishing"))) {
			//if is is finished, wait until server finishes
			ServiceLogger.log("Upload status is finished... waiting for C2S to complete the overall process...");
		}else {
			//unknown upstreamstatus code it means we crashed, notify of strange behavior to server... report?
			ServiceLogger.error("Unknown upstreamstatus code="+upstreamstatus+". Corrupt connection metadata...");
		}
		
		pauseWork(true);
		
	}
	
	private void pullToRemote() throws C2SUnreachableException,NoSAIException, InterruptedException {

		boolean downloadProducts = true;
		boolean downloadOrders = true;
		ServiceLogger.log("Pulling from remote...");
		String upstreamstatus = c2s.getUpstreamStatus();
		String entity = c2s.getEntity();
		
		if(upstreamstatus.equals("waiting")) {
			//if is waiting, good, change to initialized
			c2s.setInitializeDownload();
			upstreamstatus = c2s.getUpstreamStatus();
			//(if webonline responds error, then notify of strange behavior to server)
		}
		if(upstreamstatus.equals("initialized")) {
			//if is initialized, check cursor
			//from cursor start upload
			//when packet uploaded, renew offsets
			if(entity.equals("products")) {
				if(downloadProducts) {
					while(c2s.hasMoreUpdatedProducts(0)) {
						JSONObject product = c2s.nextProduct();
						if(product != null) {
							String id = (String) product.get("sku");
							try {
								JSONObject productstored = sai.storeProduct(product);
								c2s.sendProductPullSuccessNotification(id, productstored, true, "");
							}catch(NoSAIException e) {
								c2s.sendProductPullSuccessNotification(id, null, false, e.getMessage());
							}							
						}
					}
				}
				c2s.setFinishProductDownload();
				entity = c2s.getEntity();
			}
			if(entity.equals("orders")) {
				if(downloadOrders) {
					while(c2s.hasMoreUpdatedOrders(0)) {
						JSONObject order = c2s.nextOrder();
						String id = (String) order.get("orderid");
						try {
							JSONObject orderstored = sai.storeOrder(order);
							c2s.sendOrderPullSuccessNotification(id, orderstored, true, "");
						}catch(NoSAIException e) {
							ServiceLogger.error(e);
							c2s.sendOrderPullSuccessNotification(id, null, false, e.getMessage());
						}
					}
				}
				c2s.setFinishOrderDownload();
			}
			//when no more in SAI, send finish command
			c2s.setFinishDownload();
			upstreamstatus = c2s.getUpstreamStatus();
		}
		if(upstreamstatus.equals("finished") || upstreamstatus.equals("finishing")) {
			//if is is finished, wait until server finishes
			ServiceLogger.log("Download status is finished... waiting for C2S to complete the overall process...");
		}else {
			//unknown upstreamstatus code it means we crashed, notify of strange behavior to server... report?
			ServiceLogger.error("Unknown upstreamstatus code="+upstreamstatus+". Corrupt connection metadata...");
		}
		
		pauseWork(true);
		
	}
	
	private void work() throws C2SUnreachableException,NoSAIException,C2SRCServiceException, InterruptedException {
		
		if(checkIfWeAreFetchingRemote()) {
			fetchRemote();
		}
		if(checkIfWeArePullingToRemote()) {
			pullToRemote();
		}
		
	}
	
	private static class ShutdownHook implements Runnable {
		
		public void run() {
			onStop();
		}
		
		private void onStop() {
			ServiceLogger.error("Ended at " + new Date());
			System.out.flush();
			System.out.close();
		}
		
	}

}
