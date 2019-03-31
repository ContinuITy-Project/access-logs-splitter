package org.continuity.access.logs.splitter.combine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.continuity.access.logs.splitter.AccessLogEntry;

public class SessionsCombinerWorker implements Runnable {

	private static final long MIN_SESSION_LENGTH = 30 * 60 * 1000;

	private static final long WAIT_TIME_BETWEEN_SESSIONS = 5 * 60 * 1000;

	private final Path pathToSessions;

	private final Path outputDir;

	private final int iterationIndex;

	private final int sessionIndex;

	private final Random rand;

	private final List<AccessLogEntry> collectedSession = new ArrayList<>();

	public SessionsCombinerWorker(Path pathToSessions, Path outputDir, int iterationIndex, int sessionIndex) {
		this.pathToSessions = pathToSessions;
		this.outputDir = outputDir;
		this.iterationIndex = iterationIndex;
		this.sessionIndex = sessionIndex;
		this.rand = new Random((iterationIndex * 100) + sessionIndex);
	}

	@Override
	public void run() {
		List<String> sessions = Arrays.asList(pathToSessions.toFile().list());
		Collections.sort(sessions);

		while (isShorterThanMinLength()) {
			String session = sessions.get(rand.nextInt(sessions.size()));
			addSession(session);
		}

		writeCollectedSession();
	}

	private boolean isShorterThanMinLength() {
		return collectedSession.stream().mapToLong(AccessLogEntry::getDelay).sum() < MIN_SESSION_LENGTH;
	}

	private void addSession(String session) {
		try {
			Files.readAllLines(pathToSessions.resolve(session)).stream().skip(1).map(AccessLogEntry::fromCsvLine).forEach(collectedSession::add);
			collectedSession.get(collectedSession.size() - 1).setDelay(WAIT_TIME_BETWEEN_SESSIONS);
		} catch (IOException e) {
			System.err.println(iterationIndex + " -> " + sessionIndex + ": " + session);
			e.printStackTrace();
		}
	}

	private void writeCollectedSession() {
		List<String> lines = collectedSession.stream().map(AccessLogEntry::toString).collect(Collectors.toList());
		lines.add(0, AccessLogEntry.HEADER);

		try {
			Files.write(outputDir.resolve("iteration-" + iterationIndex).resolve("session-" + sessionIndex + ".csv"), lines, StandardOpenOption.CREATE);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
