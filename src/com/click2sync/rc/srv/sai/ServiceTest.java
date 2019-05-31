package com.click2sync.rc.srv.sai;

import org.boris.winrun4j.AbstractService;
import org.boris.winrun4j.ServiceException;

public class ServiceTest extends AbstractService {

	@Override
	public int serviceMain(String[] args) throws ServiceException {
		
		while(!shutdown) {
			Main.abstractLoop();
		}
		return 0;
	}

}