package org.continuity.access.logs.splitter;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;

public class Main {

	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length < 2) {
			throw new IllegalArgumentException("The first two arguments need to be the path to the access logs file and the output directory!");
		}

		Path pathToLogs = Paths.get(args[0]);
		Path outputDir = Paths.get(args[1]);

		AccessLogsSplitter splitter = new AccessLogsSplitter(pathToLogs, outputDir);
		splitter.parseAndSplit();
	}

	public static void main2(String[] args) throws ParseException {
		Date timestamp = DateUtils.parseDate("05/Nov/2018:08:05:22 +0100", "dd/MMM/yyyy:HH:mm:ss Z");

		System.out.println(timestamp);
	}

}
