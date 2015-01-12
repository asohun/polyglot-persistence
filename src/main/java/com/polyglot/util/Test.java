package com.polyglot.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test {

	private final static Logger log = LoggerFactory.getLogger(Test.class);

	public static void main(String[] args) {
		log.trace("Test Trace");
		log.debug("Test Debug");
		log.info("Test Info");
		log.warn("Test Warn");
		log.error("Test Error");
	}
	
}
