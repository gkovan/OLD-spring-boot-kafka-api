package com.ibm.kafkastream.window.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.ibm.kafkastream.window.service.WindowStreamService;

@WebListener
public class WindowAppContextListener implements ServletContextListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(WindowAppContextListener.class);

	@Autowired
	WindowStreamService windowStreamService;
	
	@Override
	public void contextInitialized(ServletContextEvent sce) {
		LOGGER.info("Window Stream Listener started.");
		windowStreamService.start();	
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		LOGGER.info("Window Stream Listener stopped.");
		windowStreamService.stop();
	}	
}
