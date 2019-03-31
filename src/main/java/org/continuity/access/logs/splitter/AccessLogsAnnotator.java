package org.continuity.access.logs.splitter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.continuity.commons.idpa.RequestUriMapper;
import org.continuity.idpa.annotation.ApplicationAnnotation;
import org.continuity.idpa.annotation.EndpointAnnotation;
import org.continuity.idpa.annotation.ParameterAnnotation;
import org.continuity.idpa.application.Application;
import org.continuity.idpa.application.HttpEndpoint;
import org.continuity.idpa.application.HttpParameter;
import org.continuity.idpa.application.HttpParameterType;
import org.continuity.idpa.visitor.FindBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccessLogsAnnotator {

	public static final AccessLogsAnnotator NOOP = new Noop();

	private static final Logger LOGGER = LoggerFactory.getLogger(AccessLogsAnnotator.class);

	private static final Pattern PATTERN_URL_PART_PARAM = Pattern.compile("\\{([^{]*)\\}");

	private static final String APPLICATION_JSON = "application/json";

	private static final String MULTIPART_BOUNDARY = "XXXXXXXXXXXXX";

	private static final String MULTIPART_FORM_DATA = "multipart/form-data; boundary=" + MULTIPART_BOUNDARY;

	private static final String NEWLINE = "\\r\\n";

	private final ApplicationAnnotation annotation;

	private final RequestUriMapper mapper;

	private final InputFormatter inputFormatter = new InputFormatter();

	public AccessLogsAnnotator(Application application, ApplicationAnnotation annotation) {
		this(application, annotation, true);
	}

	private AccessLogsAnnotator(Application application, ApplicationAnnotation annotation, boolean init) {
		this.annotation = annotation;
		this.mapper = new RequestUriMapper(application);

		if (init) {
			annotation.getEndpointAnnotations().stream().map(EndpointAnnotation::getParameterAnnotations).flatMap(List::stream).forEach(p -> p.getAnnotatedParameter().resolve(application));
		}
	}

	public AccessLogEntry annotateLogEntry(AccessLogEntry logEntry) {
		String[] pathAndQuery = logEntry.getPath().split("\\?");
		String path;

		if (pathAndQuery.length == 0) {
			path = logEntry.getPath();
		} else {
			path = pathAndQuery[0];
		}

		HttpEndpoint endpoint = mapper.map(path, logEntry.getRequestMethod());

		if (endpoint == null) {
			LOGGER.debug("Ignoring log entry {}", logEntry);
			return null;
		}

		EndpointAnnotation endpointAnnotation = FindBy.find(ann -> Objects.equals(endpoint.getId(), ann.getAnnotatedEndpoint().getId()), EndpointAnnotation.class).in(annotation).getFound();

		Map<String, String> inputStringPerParam = getInputStringPerParam(endpointAnnotation);
		String query = createQueryString(endpoint, inputStringPerParam);
		StringBuilder newPath = new StringBuilder().append(createAnnotatedPath(endpoint, inputStringPerParam));

		if ((query != null) && !query.isEmpty()) {
			newPath.append("?");
			newPath.append(query);
		}

		AccessLogEntry newEntry = new AccessLogEntry(logEntry.getThreadId(), logEntry.getTimestamp(), logEntry.getRequestMethod(), newPath.toString());
		addBodyIfPresent(newEntry, endpoint, inputStringPerParam);
		newEntry.setDelay(logEntry.getDelay());

		return newEntry;
	}

	private String createAnnotatedPath(HttpEndpoint endpoint, Map<String, String> inputStringPerParam) {
		String path = endpoint.getPath();
		Matcher matcher = PATTERN_URL_PART_PARAM.matcher(path);
		StringBuilder annotatedPath = new StringBuilder();

		int lastEnd = 0;

		while (matcher.find()) {
			int start = matcher.start();
			int end = matcher.end();
			String param = matcher.group(1);

			if (param.endsWith(":*")) {
				param = param.substring(0, param.length() - 2);
			}

			annotatedPath.append(path.substring(lastEnd, start));
			annotatedPath.append(inputStringPerParam.get(param));

			lastEnd = end;
		}

		annotatedPath.append(path.substring(lastEnd, path.length()));

		return annotatedPath.toString();
	}

	private String createQueryString(HttpEndpoint endpoint, Map<String, String> inputStringPerParam) {
		return endpoint.getParameters().stream().filter(p -> p.getParameterType() == HttpParameterType.REQ_PARAM).map(HttpParameter::getName)
				.map(name -> new StringBuilder().append(name).append("=").append(inputStringPerParam.get(name)).toString())
				.collect(Collectors.joining("&"));
	}

	private void addBodyIfPresent(AccessLogEntry logEntry, HttpEndpoint endpoint, Map<String, String> inputStringPerParam) {
		Optional<String> bodyParam = endpoint.getParameters().stream().filter(p -> p.getParameterType() == HttpParameterType.BODY).map(HttpParameter::getName).findFirst();
		List<String> formParams = endpoint.getParameters().stream().filter(p -> p.getParameterType() == HttpParameterType.FORM).map(HttpParameter::getName).collect(Collectors.toList());

		if (bodyParam.isPresent()) {
			logEntry.setBody(inputStringPerParam.get(bodyParam.get()).replace("\"", "\"\""));
			logEntry.setContentType(APPLICATION_JSON);
		} else if (!formParams.isEmpty()) {
			logEntry.setBody(createFormBody(formParams, inputStringPerParam));
			logEntry.setContentType(MULTIPART_FORM_DATA);
		}
	}

	private String createFormBody(List<String> formParams, Map<String, String> inputStringPerParam) {
		StringBuilder body = new StringBuilder();

		for (String formParam : formParams) {
			body.append("--").append(MULTIPART_BOUNDARY).append(NEWLINE);
			body.append("Content-Disposition: form-data; name=\"\"").append(formParam).append("\"\"").append(NEWLINE);
			body.append("Content-Type: text/plain; charset=US-ASCII").append(NEWLINE);
			body.append("Content-Transfer-Encoding: 8bit").append(NEWLINE).append(NEWLINE);
			body.append(inputStringPerParam.get(formParam)).append(NEWLINE);
		}

		body.append("--").append(MULTIPART_BOUNDARY).append("--");

		return body.toString();
	}

	private Map<String, String> getInputStringPerParam(EndpointAnnotation endpointAnnotation) {
		return endpointAnnotation.getParameterAnnotations().stream()
				.map(ann -> Pair.of(parameterNameFromAnnotation(ann), inputFormatter.getInputString(ann.getInput())))
				.collect(Collectors.toMap(Pair::getKey, Pair::getValue));
	}

	private String parameterNameFromAnnotation(ParameterAnnotation ann) {
		return ((HttpParameter) ann.getAnnotatedParameter().getReferred()).getName();
	}

	private static class Noop extends AccessLogsAnnotator {

		public Noop() {
			super(null, null, false);
		}

		@Override
		public AccessLogEntry annotateLogEntry(AccessLogEntry logEntry) {
			return logEntry;
		}

	}

}
