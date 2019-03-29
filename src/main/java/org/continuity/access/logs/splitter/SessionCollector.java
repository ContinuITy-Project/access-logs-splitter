package org.continuity.access.logs.splitter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionCollector implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(SessionCollector.class);

	private static final AccessLogEntry STOP_ENTRY = new AccessLogEntry("STOP", new Date(0), "STOP", "STOP");

	private static final long MAX_DELAY_IN_SESSION = 30 * 60 * 1000;

	private static final String FILENAME_PREFIX = "session-";

	private static final String FILENAME_EXT = ".csv";

	private final BlockingQueue<AccessLogEntry> queue = new LinkedBlockingQueue<>();

	private final String threadId;

	private final AtomicInteger sessionCounter;

	private final Path outputDir;

	private AccessLogEntry lastEntry;

	private BufferedWriter writer;

	public SessionCollector(String threadId, AtomicInteger sessionCounter, Path outputDir) {
		this.threadId = threadId;
		this.sessionCounter = sessionCounter;
		this.outputDir = outputDir;
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
				processLogEntry(logEntry);
			}
		}

		writeLastEntry();
		finishFile();
	}

	private void processLogEntry(AccessLogEntry logEntry) {
		// LOGGER.debug("Processing entry {}", logEntry);

		long delay = lastEntry == null ? 0 : logEntry.getTimestamp().getTime() - lastEntry.getTimestamp().getTime();

		if ((lastEntry != null) && (delay <= MAX_DELAY_IN_SESSION)) {
			lastEntry.setDelay(delay);
		}

		writeLastEntry();

		if (delay > MAX_DELAY_IN_SESSION) {
			finishFile();
			initFile();
		}

		lastEntry = logEntry;
	}

	private void initFile() {
		String filename = FILENAME_PREFIX + sessionCounter.getAndIncrement() + FILENAME_EXT;

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
			try {
				writer.write(lastEntry.toString());
				writer.newLine();
			} catch (IOException e) {
				LOGGER.error("Could not write CSV file row!", e);
			}
		}
	}

}
