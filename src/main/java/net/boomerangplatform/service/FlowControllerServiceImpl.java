package net.boomerangplatform.service;

import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import io.kubernetes.client.ApiException;
import net.boomerangplatform.error.BoomerangException;
import net.boomerangplatform.kube.exception.KubeRuntimeException;
import net.boomerangplatform.kube.service.FlowKubeServiceImpl;
import net.boomerangplatform.model.Response;
import net.boomerangplatform.model.Task;
import net.boomerangplatform.model.TaskCustom;
import net.boomerangplatform.model.TaskResponse;
import net.boomerangplatform.model.TaskTemplate;
import net.boomerangplatform.model.Workflow;

@Service
@Profile({"live", "local"})
public class FlowControllerServiceImpl extends AbstractControllerServiceImpl {

  private static final Logger LOGGER = LogManager.getLogger(FlowControllerServiceImpl.class);

  @Autowired
  private FlowKubeServiceImpl kubeService;

  @Autowired
  private DeleteServiceImpl deleteService;

  @Override
  public Response createWorkflow(Workflow workflow) {
    Response response = new Response("0", "Workflow Activity (" + workflow.getWorkflowActivityId()
        + ") has been created successfully.");
    try {
    	LOGGER.info(workflow.toString());
      if (workflow.getWorkflowStorage().getEnable()) {
        kubeService.createWorkflowPVC(workflow.getWorkflowName(), workflow.getWorkflowId(),
            workflow.getWorkflowActivityId(), workflow.getWorkflowStorage().getSize());
        kubeService.watchWorkflowPVC(workflow.getWorkflowId(), workflow.getWorkflowActivityId()).getPhase();
      }
      kubeService.createWorkflowConfigMap(workflow.getWorkflowName(), workflow.getWorkflowId(),
          workflow.getWorkflowActivityId(), workflow.getProperties());
      kubeService.watchConfigMap(workflow.getWorkflowId(), workflow.getWorkflowActivityId(), null);
    } catch (ApiException | KubeRuntimeException e) {
    	  throw new BoomerangException(e, 1, e.toString(), HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return response;
  }

  @Override
  public Response terminateWorkflow(Workflow workflow) {
    Response response = new Response("0", "Workflow Activity (" + workflow.getWorkflowActivityId()
        + ") has been terminated successfully.");
    try {
      kubeService.deleteWorkflowPVC(workflow.getWorkflowId(), workflow.getWorkflowActivityId());
      kubeService.deleteConfigMap(workflow.getWorkflowId(), workflow.getWorkflowActivityId(), null);
    } catch (KubeRuntimeException e) {
    	  throw new BoomerangException(e, 1, e.toString(), HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return response;
  }

  @Override
  public TaskResponse executeTask(Task task) {
	  if (task instanceof TaskTemplate) {
		  return executeTaskTemplate((TaskTemplate)task);
	  } else if (task instanceof TaskCustom) {
		  return executeTaskCustom((TaskCustom)task);
	  } else {
		  throw new BoomerangException(1,"UNKOWN_TASK_TYPE",HttpStatus.BAD_REQUEST, task.getClass().toString());
	  }
  }

	private TaskResponse executeTaskTemplate(TaskTemplate task) {
		TaskResponse response = new TaskResponse("0", "Task (" + task.getTaskId() + ") has been executed successfully.",
				null);
		if (task.getImage() == null) {
			throw new BoomerangException(1, "NO_TASK_IMAGE", HttpStatus.BAD_REQUEST, task.getClass().toString());
		} else {
			try {
				kubeService.createTaskConfigMap(task.getWorkflowName(), task.getWorkflowId(),
						task.getWorkflowActivityId(), task.getTaskName(), task.getTaskId(), task.getProperties());
				kubeService.watchConfigMap(null, task.getWorkflowActivityId(), task.getTaskId());
				boolean createWatchLifecycle = task.getArguments().contains("shell") ? Boolean.TRUE : Boolean.FALSE;
				kubeService.createJob(createWatchLifecycle, task.getWorkflowName(), task.getWorkflowId(),
						task.getWorkflowActivityId(), task.getTaskActivityId(), task.getTaskName(), task.getTaskId(),
						task.getArguments(), task.getProperties(), task.getImage(), task.getCommand(),
						task.getConfiguration());
				kubeService.watchJob(createWatchLifecycle, task.getWorkflowId(), task.getWorkflowActivityId(),
						task.getTaskId());
			} catch (KubeRuntimeException e) {
				LOGGER.info("DEBUG::Task Is Being Set as Failed");
				  throw new BoomerangException(e, 1, e.toString(), HttpStatus.INTERNAL_SERVER_ERROR);
			} finally {
				response.setOutput(kubeService.getTaskOutPutConfigMapData(task.getWorkflowId(),
						task.getWorkflowActivityId(), task.getTaskId(), task.getTaskName()));
				kubeService.deleteConfigMap(null, task.getWorkflowActivityId(), task.getTaskId());
				if (isTaskDeletionNever(task.getConfiguration().getDeletion())) {
				  deleteService.deleteJob(getTaskDeletion(task.getConfiguration().getDeletion()), task.getWorkflowId(),
							task.getWorkflowActivityId(), task.getTaskId());
				}
				LOGGER.info("Task (" + task.getTaskId() + ") has completed with code " + response.getCode());
			}
		}
		return response;
	}

  private TaskResponse executeTaskCustom(TaskCustom task) {
		TaskResponse response = new TaskResponse("0", "Task (" + task.getTaskId() + ") has been executed successfully.",
				null);
		if (task.getImage() == null) {
			throw new BoomerangException(1, "NO_TASK_IMAGE", HttpStatus.BAD_REQUEST, task.getClass().toString());
		} else {
			try {
				kubeService.createTaskConfigMap(task.getWorkflowName(), task.getWorkflowId(),
						task.getWorkflowActivityId(), task.getTaskName(), task.getTaskId(), task.getProperties());
				kubeService.watchConfigMap(null, task.getWorkflowActivityId(), task.getTaskId());
				kubeService.createJob(true, task.getWorkflowName(), task.getWorkflowId(), task.getWorkflowActivityId(),
						task.getTaskActivityId(), task.getTaskName(), task.getTaskId(), task.getArguments(),
						task.getProperties(), task.getImage(), task.getCommand(), task.getConfiguration());
				kubeService.watchJob(true, task.getWorkflowId(), task.getWorkflowActivityId(), task.getTaskId());
			} catch (KubeRuntimeException e) {
				LOGGER.info("DEBUG::Task Is Being Set as Failed");
				throw new BoomerangException(e, 1, e.toString(), HttpStatus.INTERNAL_SERVER_ERROR);
			} finally {
				response.setOutput(kubeService.getTaskOutPutConfigMapData(task.getWorkflowId(),
						task.getWorkflowActivityId(), task.getTaskId(), task.getTaskName()));
				kubeService.deleteConfigMap(null, task.getWorkflowActivityId(), task.getTaskId());
				if (isTaskDeletionNever(task.getConfiguration().getDeletion())) {
                  deleteService.deleteJob(getTaskDeletion(task.getConfiguration().getDeletion()), task.getWorkflowId(),
                      task.getWorkflowActivityId(), task.getTaskId());
				}
				LOGGER.info("Task (" + task.getTaskId() + ") has completed with code " + response.getCode());
			}
		}
		return response;
  }

  @Override
  public Response setJobOutputProperty(String workflowId, String workflowActivityId, String taskId,
      String taskName, String key, String value) {
    Response response = new Response("0", "Property has been set against workflow ("
        + workflowActivityId + ") and task (" + taskId + ")");
    try {
      Map<String, String> properties = new HashMap<>();
      properties.put(key, value);
      kubeService.patchTaskConfigMap(workflowId, workflowActivityId, taskId, taskName, properties);
    } catch (KubeRuntimeException e) {
  	  throw new BoomerangException(e, 1, e.toString(), HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return response;
  }

  @Override
  public Response setJobOutputProperties(String workflowId, String workflowActivityId,
      String taskId, String taskName, Map<String, String> properties) {
    Response response = new Response("0", "Properties have been set against workflow ("
        + workflowActivityId + ") and task (" + taskId + ")");

    LOGGER.info(properties);
    try {
      kubeService.patchTaskConfigMap(workflowId, workflowActivityId, taskId, taskName, properties);
    } catch (KubeRuntimeException e) {
  	  throw new BoomerangException(e, 1, e.toString(), HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return response;
  }
}
