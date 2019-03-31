package org.continuity.access.logs.splitter;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.continuity.access.logs.splitter.combine.SessionsCombiner;
import org.continuity.access.logs.splitter.split.AccessLogsAnnotator;
import org.continuity.access.logs.splitter.split.AccessLogsSplitter;
import org.continuity.idpa.annotation.ApplicationAnnotation;
import org.continuity.idpa.application.Application;
import org.continuity.idpa.serialization.yaml.IdpaYamlSerializer;

public class Main {

	public static void main(String[] args) throws IOException, InterruptedException {
		switch (args[0]) {
		case "split":
			splitMain(Arrays.copyOfRange(args, 1, args.length));
			break;
		case "combine":
			combineMain(Arrays.copyOfRange(args, 1, args.length));
			break;
		default:
			throw new IllegalArgumentException("Unknown command " + args[0]);
		}
	}

	private static void splitMain(String[] args) throws IOException, InterruptedException {
		if (args.length < 2) {
			throw new IllegalArgumentException("The first two arguments after the command need to be the path to the access logs file and the output directory!");
		}

		Path pathToLogs = Paths.get(args[0]);
		Path outputDir = Paths.get(args[1]);

		AccessLogsSplitter splitter;
		AccessLogsAnnotator annotator;

		if (args.length >= 5) {
			Application application = new IdpaYamlSerializer<>(Application.class).readFromYaml(args[3]);
			ApplicationAnnotation annotation = new IdpaYamlSerializer<>(ApplicationAnnotation.class).readFromYaml(args[4]);
			annotator = new AccessLogsAnnotator(application, annotation);
		} else {
			annotator = AccessLogsAnnotator.NOOP;
		}

		if (args.length >= 3) {
			Path pathToIgnored = Paths.get(args[2]);
			splitter = new AccessLogsSplitter(pathToLogs, outputDir, annotator, pathToIgnored);
		} else {
			splitter = new AccessLogsSplitter(pathToLogs, outputDir, annotator);
		}

		splitter.parseAndSplit();
	}

	private static void combineMain(String[] args) throws InterruptedException {
		if (args.length < 4) {
			throw new IllegalArgumentException(
					"The first four arguments after the command need to be the path to the sessions CSV files, the output directory, the number of parallel sessions, and the number of iterations!");
		}

		Path pathToSessions = Paths.get(args[0]);
		Path outputDir = Paths.get(args[1]);
		int numSessions = Integer.parseInt(args[2]);
		int numIterations = Integer.parseInt(args[3]);

		SessionsCombiner combiner = new SessionsCombiner(pathToSessions, outputDir, numSessions, numIterations);
		combiner.combine();
	}

}
