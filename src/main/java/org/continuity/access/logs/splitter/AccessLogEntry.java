package org.continuity.access.logs.splitter;

import java.text.ParseException;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.time.DateUtils;

public class AccessLogEntry {

	public static final String HEADER = "\"delay\",\"method\",\"request\",\"contenttype\",\"body\"";

	private static final Pattern ACCESS_LOGS_PATTERN = Pattern.compile("(.*) - - \\[([^\\]]+)\\] \"([A-Z]+) ([^\"]+) .+\" .*");

	private static final String DELIMITER = ",";

	private static final String ESCAPER = "\"";

	private final String threadId;

	private final Date timestamp;

	private final String requestMethod;

	private final String path;

	private long delay;

	private String contentType = "*/*";

	private String body = "";

	public AccessLogEntry(String threadId, Date timestamp, String requestMethod, String path) {
		this.threadId = threadId;
		this.timestamp = timestamp;
		this.requestMethod = requestMethod;
		this.path = path;
	}

	public static AccessLogEntry fromLogLine(String line) throws ParseException {
		Matcher matcher = ACCESS_LOGS_PATTERN.matcher(line);

		if (matcher.find()) {
			Date timestamp = DateUtils.parseDateStrictly(matcher.group(2), Locale.ENGLISH, "dd/MMM/yyyy:HH:mm:ss Z");
			return new AccessLogEntry(matcher.group(1), timestamp, matcher.group(3), matcher.group(4));
		} else {
			return null;
		}
	}

	public String getThreadId() {
		return threadId;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public String getRequestMethod() {
		return requestMethod;
	}

	public String getPath() {
		return path;
	}

	public long getDelay() {
		return delay;
	}

	public void setDelay(long delay) {
		this.delay = delay;
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	@Override
	public String toString() {
		return new StringBuilder().append(ESCAPER).append(getDelay()).append(ESCAPER).append(DELIMITER).append(ESCAPER).append(getRequestMethod()).append(ESCAPER).append(DELIMITER).append(ESCAPER)
				.append(getPath()).append(ESCAPER).append(DELIMITER).append(ESCAPER).append(getContentType()).append(ESCAPER).append(DELIMITER).append(ESCAPER).append(getBody()).append(ESCAPER)
				.toString();
	}

}
