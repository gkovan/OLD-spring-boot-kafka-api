package com.ibm.kafkastream.json.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.ibm.kafkastream.json.service.StreamService;

@WebListener
public class StreamAppContextListener implements ServletContextListener {

	
	private static final Logger LOGGER = LoggerFactory.getLogger(StreamAppContextListener.class);

	@Autowired
	StreamService streamService;

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		LOGGER.info("Stream Listener started.");
		streamService.start();	
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		LOGGER.info("Stream Listener stopped.");
		streamService.stop();
	}	

}
