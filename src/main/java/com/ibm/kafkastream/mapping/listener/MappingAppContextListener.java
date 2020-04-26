package com.ibm.kafkastream.mapping.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.ibm.kafkastream.mapping.service.MappingStreamApp;
import com.ibm.kafkastream.wordcount.listener.WordCountAppContextListener;

@WebListener
public class MappingAppContextListener implements ServletContextListener { 
	private static final Logger LOGGER = LoggerFactory.getLogger(WordCountAppContextListener.class);
	
	@Autowired
	MappingStreamApp mappingStreamService;
	
	@Override
	public void contextInitialized(ServletContextEvent sce) {
		LOGGER.info("Pipe Stream Listener started.");
		mappingStreamService.start();	
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		LOGGER.info("Pipe Stream Listener stopped.");
		mappingStreamService.stop();
	}	

}
