package net.boomerangplatform.service;

import javax.servlet.http.HttpServletResponse;

import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

public abstract interface LogService {

//	Response getLogForTask(String workflowId, String workflowActivityId, String taskId,  String taskActivityId);

	StreamingResponseBody streamLogForTask(HttpServletResponse response, String workflowId, String workflowActivityId,
			String taskId,  String taskActivityId);
}