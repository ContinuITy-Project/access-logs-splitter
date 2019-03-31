package org.continuity.access.logs.splitter.split;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.continuity.access.logs.splitter.AccessLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionCollector implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(SessionCollector.class);

	private static final AccessLogEntry STOP_ENTRY = new AccessLogEntry("STOP", new Date(0), "STOP", "STOP");

	private static final long MAX_DELAY_IN_SESSION = 30 * 60 * 1000;

	private static final long WAIT_TIME_BETWEEN_SESSIONS = 30 * 1000;

	private static final String FILENAME_PREFIX = "session-";

	private static final String FILENAME_EXT = ".csv";

	private static final Set<String> POST_PUT = Stream.of("POST", "PUT").collect(Collectors.toSet());

	private final BlockingQueue<AccessLogEntry> queue = new LinkedBlockingQueue<>();

	private final String threadId;

	private final Path outputDir;

	private final AccessLogsAnnotator annotator;

	private AccessLogEntry lastEntry;

	private BufferedWriter writer;

	private String filename;

	private int sessionCounter = 1;

	public SessionCollector(String threadId, Path outputDir, AccessLogsAnnotator annotator) {
		this.threadId = threadId;
		this.outputDir = outputDir;
		this.annotator = annotator;
	}

	public void offer(AccessLogEntry entry) {
		queue.offer(entry);
	}

	public void stop() {
		queue.offer(STOP_ENTRY);
	}

	@Override
	public void run() {
		initFile();

		while (true) {
			AccessLogEntry logEntry;

			try {
				logEntry = queue.take();
			} catch (InterruptedException e) {
				LOGGER.error("Interrupted!", e);
				continue;
			}

			if (STOP_ENTRY.equals(logEntry)) {
				break;
			} else {
				processLogEntry(annotator.annotateLogEntry(logEntry));
			}
		}

		lastEntry.setDelay(WAIT_TIME_BETWEEN_SESSIONS);
		writeLastEntry();
		finishFile();
	}

	private void processLogEntry(AccessLogEntry logEntry) {
		// LOGGER.debug("Processing entry {}", logEntry);

		long delay = lastEntry == null ? 0 : logEntry.getTimestamp().getTime() - lastEntry.getTimestamp().getTime();

		if ((lastEntry != null)) {
			lastEntry.setDelay(Math.min(delay, WAIT_TIME_BETWEEN_SESSIONS));
		}

		writeLastEntry();

		if (delay > MAX_DELAY_IN_SESSION) {
			finishFile();
			initFile();
		}

		lastEntry = logEntry;
	}

	private void initFile() {
		filename = new StringBuilder().append(FILENAME_PREFIX).append(threadId.hashCode()).append("-").append(sessionCounter++).append(FILENAME_EXT).toString();

		LOGGER.info("Writing session of thread {} to {}", threadId, filename);

		try {
			writer = Files.newBufferedWriter(outputDir.resolve(filename), StandardOpenOption.CREATE);
		} catch (IOException e) {
			LOGGER.error("Could not initialize file!", e);
		}

		try {
			writer.write(AccessLogEntry.HEADER);
			writer.newLine();
		} catch (IOException e) {
			LOGGER.error("Could not write CSV file header!", e);
		}
	}

	private void finishFile() {
		try {
			writer.close();
		} catch (IOException e) {
			LOGGER.error("Could not close the writer!", e);
		}
	}

	private void writeLastEntry() {
		if (lastEntry != null) {
			if (POST_PUT.contains(lastEntry.getRequestMethod())) {
				LOGGER.info("{} {} is contained in {}.", lastEntry.getRequestMethod(), lastEntry.getPath(), filename);
			}

			try {
				writer.write(lastEntry.toString());
				writer.newLine();
			} catch (IOException e) {
				LOGGER.error("Could not write CSV file row!", e);
			}
		}
	}

}
