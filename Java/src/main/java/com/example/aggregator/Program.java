package com.example.aggregator;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Program {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(Program.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {
        LOGGER.info("press CTRL-C to close.");
        var charCounter = new CharCounter();
        var future = charCounter.startAsync();
        LOGGER.info("Started aggregator");
        future.get();
        LOGGER.info("Terminated");
    }


}
