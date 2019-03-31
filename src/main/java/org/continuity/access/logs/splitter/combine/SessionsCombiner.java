package org.continuity.access.logs.splitter.combine;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionsCombiner {

	private static final Logger LOGGER = LoggerFactory.getLogger(SessionsCombiner.class);

	private final Path pathToSessions;

	private final Path outputDir;

	private final int numSessions;

	private final int numIterations;

	private final ExecutorService threadPool = Executors.newCachedThreadPool();

	public SessionsCombiner(Path pathToSessions, Path outputDir, int numSessions, int numIterations) {
		this.pathToSessions = pathToSessions;
		this.outputDir = outputDir;
		this.numSessions = numSessions;
		this.numIterations = numIterations;
	}

	public void combine() throws InterruptedException {
		for (int iteration = 1; iteration <= numIterations; iteration++) {
			LOGGER.info("Combining for iteration {}...", iteration);
			combineIteration(iteration);
		}

		threadPool.shutdown();
		threadPool.awaitTermination(1, TimeUnit.MINUTES);
	}

	private void combineIteration(int iteration) {
		outputDir.resolve("iteration-" + iteration).toFile().mkdirs();

		for (int session = 1; session <= numSessions; session++) {
			SessionsCombinerWorker worker = new SessionsCombinerWorker(pathToSessions, outputDir, iteration, session);
			threadPool.execute(worker);
		}
	}

}
