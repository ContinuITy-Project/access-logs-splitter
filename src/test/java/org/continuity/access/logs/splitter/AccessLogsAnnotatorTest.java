package org.continuity.access.logs.splitter;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Date;

import org.continuity.idpa.annotation.ApplicationAnnotation;
import org.continuity.idpa.application.Application;
import org.continuity.idpa.serialization.yaml.IdpaYamlSerializer;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class AccessLogsAnnotatorTest {

	private Application application;

	private ApplicationAnnotation annotation;

	private AccessLogsAnnotator annotator;

	@Before
	public void setup() throws JsonParseException, JsonMappingException, IOException {
		IdpaYamlSerializer<Application> appSerializer = new IdpaYamlSerializer<>(Application.class);
		application = appSerializer.readFromYamlInputStream(getClass().getResourceAsStream("/application.yml"));
		IdpaYamlSerializer<ApplicationAnnotation> annSerializer = new IdpaYamlSerializer<>(ApplicationAnnotation.class);
		annotation = annSerializer.readFromYamlInputStream(getClass().getResourceAsStream("/annotation.yml"));

		annotator = new AccessLogsAnnotator(application, annotation);
	}

	@Test
	public void test() {
		assertThat(annotator.annotateLogEntry(createLogEntry("/foo/blub/whatever")).getPath()).isEqualTo("/foo/${__GetRandomString(${Input_bar},;)}/${Input_rest}?id=${Input_id}");
	}

	private AccessLogEntry createLogEntry(String path) {
		return new AccessLogEntry("tid", new Date(0), "GET", path);
	}

}
