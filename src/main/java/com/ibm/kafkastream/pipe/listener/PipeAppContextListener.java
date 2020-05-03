package com.ibm.kafkastream.pipe.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.ibm.kafkastream.pipe.service.PipeStreamService;

@WebListener
public class PipeAppContextListener implements ServletContextListener {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(PipeAppContextListener.class);

	@Autowired
	PipeStreamService pipeStreamService;

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		LOGGER.info("Pipe Stream Listener started.");
		pipeStreamService.start();	
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		LOGGER.info("Pipe Stream Listener stopped.");
		pipeStreamService.stop();
	}	


}
	
	
