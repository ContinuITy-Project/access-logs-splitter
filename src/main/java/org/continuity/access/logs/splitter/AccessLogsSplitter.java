package org.continuity.access.logs.splitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses an access logs file and splits it into one CSV file per session. A session is determined
 * by the thread ID and the delays between the requests.
 *
 * @author Henning Schulz
 *
 */
public class AccessLogsSplitter {

	private static final Logger LOGGER = LoggerFactory.getLogger(AccessLogsSplitter.class);

	private final Path pathToLogs;

	private final Path outputDir;

	private final AtomicInteger sessionCounter = new AtomicInteger(1);

	private final Map<String, SessionCollector> collectorPerThread = new ConcurrentHashMap<>();

	private final ExecutorService threadPool = Executors.newCachedThreadPool();

	public AccessLogsSplitter(Path pathToLogs, Path outputDir) {
		this.pathToLogs = pathToLogs;
		this.outputDir = outputDir;
	}

	public void parseAndSplit() throws IOException, InterruptedException {
		LOGGER.info("Parsing access logs {}", pathToLogs);
		LOGGER.info("Writing sessions to {}", outputDir);

		outputDir.toFile().mkdirs();

		BufferedReader reader = Files.newBufferedReader(pathToLogs);

		String line = reader.readLine();

		while (line != null) {
			AccessLogEntry logEntry;
			try {
				logEntry = AccessLogEntry.fromLogLine(line);
			} catch (ParseException e) {
				LOGGER.error("Cannot parse date!", e);
				logEntry = null;
			}

			if (logEntry == null) {
				LOGGER.error("Cannot parse line {}", line);
			}

			collectorForThread(logEntry.getThreadId()).offer(logEntry);

			line = reader.readLine();
		}

		collectorPerThread.values().forEach(SessionCollector::stop);

		threadPool.shutdown();
		threadPool.awaitTermination(1, TimeUnit.MINUTES);
	}

	private SessionCollector collectorForThread(String threadId) {
		SessionCollector collector = collectorPerThread.get(threadId);

		if (collector == null) {
			collector = new SessionCollector(threadId, sessionCounter, outputDir);
			collectorPerThread.put(threadId, collector);
			threadPool.execute(collector);
		}

		return collector;
	}

}
