package com.cuyan.indexer;

import com.cuyan.indexer.service.TickService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class IndexerApplication {

	static Logger logger = LoggerFactory.getLogger(IndexerApplication.class);

	public static final long MSECS_PER_SECOND = 1000L;
	public static final long SLIDINGWINDOW_DURATION_SECS = 60;
	public static final long TIMER_REFRESH_MSECS = 1000L;

	public static final int MAX_QUEUE_SIZE = 1024;

	public static void main(String[] args) {

		SpringApplication.run(IndexerApplication.class, args);

		logger.info("SLIDINGWINDOW_DURATION_SECS: " + SLIDINGWINDOW_DURATION_SECS);
		logger.info("TIMER_REFRESH_MSECS: " + TIMER_REFRESH_MSECS);
		logger.info("MAX_QUEUE_SIZE: " + MAX_QUEUE_SIZE);

	}

}
