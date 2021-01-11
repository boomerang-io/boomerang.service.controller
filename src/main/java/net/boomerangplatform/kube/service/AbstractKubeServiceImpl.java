package net.boomerangplatform.kube.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.Exec;
import io.kubernetes.client.PodLogs;
import io.kubernetes.client.apis.BatchV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1Affinity;
import io.kubernetes.client.models.V1ConfigMap;
import io.kubernetes.client.models.V1ConfigMapList;
import io.kubernetes.client.models.V1ConfigMapProjection;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ContainerStatus;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1Job;
import io.kubernetes.client.models.V1JobList;
import io.kubernetes.client.models.V1JobStatus;
import io.kubernetes.client.models.V1LabelSelector;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1PersistentVolumeClaim;
import io.kubernetes.client.models.V1PersistentVolumeClaimList;
import io.kubernetes.client.models.V1PersistentVolumeClaimSpec;
import io.kubernetes.client.models.V1PersistentVolumeClaimStatus;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodAffinityTerm;
import io.kubernetes.client.models.V1PodAntiAffinity;
import io.kubernetes.client.models.V1PodCondition;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.kubernetes.client.models.V1SecurityContext;
import io.kubernetes.client.models.V1Status;
import io.kubernetes.client.models.V1Toleration;
import io.kubernetes.client.models.V1Volume;
import io.kubernetes.client.models.V1VolumeMount;
import io.kubernetes.client.models.V1VolumeProjection;
import io.kubernetes.client.models.V1WeightedPodAffinityTerm;
import io.kubernetes.client.util.Watch;
import net.boomerangplatform.error.BoomerangException;
import net.boomerangplatform.kube.exception.KubeRuntimeException;
import net.boomerangplatform.model.TaskConfiguration;
import net.boomerangplatform.model.TaskDeletionEnum;
import net.boomerangplatform.service.ConfigurationService;

public abstract class AbstractKubeServiceImpl implements AbstractKubeService { // NOSONAR

  private static final Logger LOGGER = LogManager.getLogger(AbstractKubeService.class);

  private static final int PROPERTY_SIZE = 2;

  private static final Pattern PATTERN_PROPERTIES = Pattern.compile("\\s*\\n\\s*");

  private static final int TIMEOUT = 600;

  private static final int BYTE_SIZE = 1024;

  protected static final int TIMEOUT_ONE_MINUTE = 60;

  private static final String EXCEPTION = "Exception: ";

  protected static final Integer ONE_DAY_IN_SECONDS = 86400; // 60*60*24
  
  private static final String[] waitingErrorReasons = new String[] {"CrashLoopBackOff","ErrImagePull","ImagePullBackOff","CreateContainerConfigError","InvalidImageName","CreateContainerError"};

  @Value("${kube.api.base.path}")
  protected String kubeApiBasePath;

  @Value("${kube.api.token}")
  protected String kubeApiToken;

  @Value("${kube.api.debug}")
  protected Boolean kubeApiDebug;

  @Value("${kube.api.type}")
  protected String kubeApiType;

  @Value("${kube.api.pretty}")
  protected String kubeApiPretty;

  @Value("${kube.api.includeunitialized}")
  protected Boolean kubeApiIncludeuninitialized;

  @Value("${kube.api.timeout}")
  private Integer kubeApiTimeOut;

  @Value("${kube.namespace}")
  protected String kubeNamespace;

  @Value("${kube.image.pullPolicy}")
  protected String kubeImagePullPolicy;

  @Value("${kube.image.pullSecret}")
  protected String kubeImagePullSecret;

  @Value("${kube.workflow.pvc.size}")
  protected String kubeWorkerPVCSize;

  @Value("${kube.worker.job.backOffLimit}")
  protected Integer kubeWorkerJobBackOffLimit;

  @Value("${kube.worker.job.restartPolicy}")
  protected String kubeWorkerJobRestartPolicy;

  @Value("${kube.worker.job.ttlDays}")
  protected Integer kubeWorkerJobTTLDays;

  @Value("${kube.worker.hostaliases}")
  protected String kubeWorkerHostAliases;

  @Value("${kube.worker.node.dedicated}")
  protected Boolean kubeWorkerDedicatedNodes;

  @Value("${kube.worker.serviceaccount}")
  protected String kubeWorkerServiceAccount;

  @Value("${proxy.enable}")
  protected Boolean proxyEnabled;

  @Value("${proxy.host}")
  protected String proxyHost;

  @Value("${proxy.port}")
  protected String proxyPort;

  @Value("${proxy.ignore}")
  protected String proxyIgnore;

  @Value("${controller.service.host}")
  protected String bmrgControllerServiceURL;

  @Autowired
  private ConfigurationService configurationService;


  private ApiClient apiClient; // NOSONAR

  protected abstract String getLabelSelector(String workflowId, String workflowActivityId,
      String taskId);

  protected abstract V1Job createJobBody(boolean createLifecycle, String workflowName,
      String workflowId, String workflowActivityId, String taskActivityId, String taskName,
      String taskId, List<String> arguments, Map<String, String> taskProperties, String image,
      String command, TaskConfiguration taskConfiguration);

  protected abstract V1ConfigMap createTaskConfigMapBody(String workflowName, String workflowId,
      String workflowActivityId, String taskName, String taskId, Map<String, String> inputProps);

  protected abstract Map<String, String> createAnnotations(String workflowName, String workflowId,
      String workflowActivityId, String taskId);

  protected abstract Map<String, String> createLabels(String workflowId, String workflowActivityId,
      String taskId);

  protected abstract V1ConfigMap createWorkflowConfigMapBody(String workflowName, String workflowId,
      String workflowActivityId, Map<String, String> inputProps);

  public abstract String getJobPrefix();

  public abstract String getPVCPrefix();

  @Override
  public V1Job createJob(boolean createLifecycle, String workflowName, String workflowId,
      String workflowActivityId, String taskActivityId, String taskName, String taskId,
      List<String> arguments, Map<String, String> taskProperties, String image, String command,
      TaskConfiguration taskConfiguration) {
    V1Job body =
        createJobBody(createLifecycle, workflowName, workflowId, workflowActivityId, taskActivityId,
            taskName, taskId, arguments, taskProperties, image, command, taskConfiguration);

    LOGGER.info(body);

    V1Job jobResult = new V1Job();
    try {
      jobResult = getBatchApi().createNamespacedJob(kubeNamespace, body,
          kubeApiIncludeuninitialized, kubeApiPretty, null);
      LOGGER.info(jobResult);
    } catch (ApiException e) {
      if (e.getCause() instanceof SocketTimeoutException) {
        SocketTimeoutException ste = (SocketTimeoutException) e.getCause();
        if (ste.getMessage() != null && ste.getMessage().contains("timeout")) {
          LOGGER.warn("Catching timeout and return as task error");
          V1JobStatus badStatus = new V1JobStatus();
          return body.status(badStatus.failed(1));
        }
      } else {
        LOGGER.debug("Create Job Exception Response Body: " + e.getResponseBody());
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode;
        try {
          jsonNode = objectMapper.readTree(e.getResponseBody());
          String exceptionMessage = jsonNode.get("message").asText();
          if (exceptionMessage.contains("admission webhook")) {
            throw new BoomerangException(1, "ADMISSION_WEBHOOK_DENIED", HttpStatus.BAD_REQUEST,
                exceptionMessage);
          }
        } catch (IOException e1) {
          // TODO Auto-generated catch block
          LOGGER.warn("Unable to parse ResponseBody as JSON. Defaulting to standard exception.");
        }
        throw new KubeRuntimeException("Error createJob", e);
      }
    }

    return jobResult;
  }

  public V1Job watchJob(boolean watchLifecycle, String workflowId, String workflowActivityId,
      String taskId) {
    String labelSelector = getLabelSelector(workflowId, workflowActivityId, taskId);
    V1Job jobResult = null;

    // Since upgrade to Java11 the watcher stops listening for events (irrespective of timeout) and
    // does not throw exception.
    // Loop will restart watcher based on our own timer
    Integer loopCount = 1;
    long endTime = System.nanoTime()
        + TimeUnit.NANOSECONDS.convert(kubeApiTimeOut.longValue(), TimeUnit.SECONDS);
    do {
      LOGGER.info("Starting Job Watcher #" + loopCount + " for Task (" + taskId + ")...");
      try {
        Watch<V1Job> watch = createJobWatch(getBatchApi(), labelSelector);
        if (watchLifecycle) {
          Watch<V1Pod> podWatch = createPodWatch(labelSelector, getCoreApi());
          getJobPod(podWatch);
        }
        jobResult = getJobResult(taskId, watch);
      } catch (ApiException | IOException e) {
        LOGGER.error("getWatch Exception: ", e);
        throw new KubeRuntimeException("Error createWatch", e);
      }
      loopCount++;
    } while (System.nanoTime() < endTime && jobResult == null);
    if (jobResult == null) {
      // Final catch for a timeout and job still not complete.
      throw new KubeRuntimeException(
          "Task (" + taskId + ") has exceeded the maximum duration triggering failure.");
    }
    return jobResult;
  }

  private void getJobPod(Watch<V1Pod> watch) throws IOException {
    V1Pod pod = null;
    try {
      for (Watch.Response<V1Pod> item : watch) {

        String name = item.object.getMetadata().getName();
        LOGGER
            .info("Pod: " + name + ", started: " + item.object.getStatus().getStartTime());
        String phase = item.object.getStatus().getPhase();
        LOGGER.info("Pod Phase: " + phase + "...");
        if (item.object.getStatus().getConditions() != null) {
          for (V1PodCondition condition : item.object.getStatus().getConditions()) {
            LOGGER.info("Pod Condition: " + condition.toString());
          }
        }
        if (item.object.getStatus().getContainerStatuses() != null) {
          for (V1ContainerStatus containerStatus : item.object.getStatus().getContainerStatuses()) {
            LOGGER.info("Container Status: " + containerStatus.toString());
            if ("worker-cntr".equalsIgnoreCase(containerStatus.getName())
                && containerStatus.getState().getTerminated() != null) {
              LOGGER.info("-----------------------------------------------");
              LOGGER.info("------- Executing Lifecycle Termination -------");
              LOGGER.info("-----------------------------------------------");
              try {
                execJobLifecycle(name, "lifecycle-cntr");
              } catch (Exception e) {
                LOGGER.error("Lifecycle Execution Exception: ", e);
                throw new KubeRuntimeException("Lifecycle Execution Exception", e);
              }
              pod = item.object;
              break;
            } else if ("worker-cntr".equalsIgnoreCase(containerStatus.getName())
                && containerStatus.getState().getWaiting() != null 
//                && "CreateContainerError".equalsIgnoreCase(containerStatus.getState().getWaiting().getReason())) {
                  && ArrayUtils.contains(waitingErrorReasons, containerStatus.getState().getWaiting().getReason())) {
              throw new KubeRuntimeException("Container Waiting Error (" + containerStatus.getState().getWaiting().getReason() + ")");
            }
          }
        }
        if (pod != null) {
          LOGGER.info("Exiting Lifecycle Termination");
          break;
        }
      }
    } finally {
      watch.close();
    }
  }

  private void execJobLifecycle(String podName, String containerName)
      throws ApiException, IOException, InterruptedException {
    Exec exec = new Exec();
    exec.setApiClient(Configuration.getDefaultApiClient());
    // boolean tty = System.console() != null;
    // String[] commands = new String[] {"node", "cli", "lifecycle", "terminate"};
    String[] commands =
        new String[] {"/bin/sh", "-c", "rm -f /lifecycle/lock && ls -ltr /lifecycle"};
    LOGGER.info("Pod: " + podName + ", Container: " + containerName + ", Commands: "
        + Arrays.toString(commands));
    final Process proc = exec.exec(kubeNamespace, podName, commands, containerName, false, false);

    // Thread in =
    // new Thread(
    // new Runnable() {
    // public void run() {
    // try {
    // ByteStreams.copy(System.in, proc.getOutputStream());
    // } catch (IOException ex) {
    // ex.printStackTrace();
    // }
    // }
    // });
    // in.start();

    Thread out = new Thread(new Runnable() {
      public void run() {
        try {
          ByteStreams.copy(proc.getInputStream(), System.out);
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      }
    });
    out.start();

    proc.waitFor();
    // wait for any last output; no need to wait for input thread
    out.join();
    proc.destroy();
  }

  @Override
  public String getPodLog(String workflowId, String workflowActivityId, String taskId,
      String taskActivityId) {
    String labelSelector = getLabelSelector(workflowId, workflowActivityId, taskId);

    PodLogs logs = new PodLogs();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      List<V1Pod> listOfPods =
          getCoreApi().listNamespacedPod(kubeNamespace, kubeApiIncludeuninitialized, kubeApiPretty,
              null, null, labelSelector, null, null, TIMEOUT_ONE_MINUTE, false).getItems();

      V1Pod pod;

      if (!listOfPods.isEmpty()) {
        pod = listOfPods.get(0);
        InputStream is = logs.streamNamespacedPodLog(pod.getMetadata().getNamespace(),
            pod.getMetadata().getName(), "worker-cntr");

        ByteStreams.copy(is, baos);
      }
    } catch (ApiException | IOException e) {
      LOGGER.error("getPodLog Exception: ", e);
      throw new KubeRuntimeException("Error getPodLog", e);
    }

    return baos.toString(StandardCharsets.UTF_8);
  }

  public boolean isKubePodAvailable(String workflowId, String workflowActivityId, String taskId) {
    String labelSelector = getLabelSelector(workflowId, workflowActivityId, taskId);

    try {
      List<V1Pod> allPods =
          getCoreApi().listNamespacedPod(kubeNamespace, kubeApiIncludeuninitialized, kubeApiPretty,
              null, null, labelSelector, null, null, TIMEOUT_ONE_MINUTE, false).getItems();

      if (allPods.isEmpty() || "succeeded".equalsIgnoreCase(allPods.get(0).getStatus().getPhase())
          || "failed".equalsIgnoreCase(allPods.get(0).getStatus().getPhase())) {
        LOGGER.info("isKubePodAvailable() - Not available");
        return false;
      }
      LOGGER.info("isKubePodAvailable() - Available");
    } catch (ApiException e) {
      LOGGER.error("streamPodLog Exception: ", e);
      throw new KubeRuntimeException("Error streamPodLog", e);
    }
    return true;
  }

  @Override
  public StreamingResponseBody streamPodLog(HttpServletResponse response, String workflowId,
      String workflowActivityId, String taskId, String taskActivityId) {

    LOGGER.info("Stream logs from Kubernetes");

    String labelSelector = getLabelSelector(workflowId, workflowActivityId, taskId);
    StreamingResponseBody responseBody = null;
    try {
      Watch<V1Pod> watch = createPodWatch(labelSelector, getCoreApi());
      V1Pod pod = getPod(watch);

      if (pod == null) {
        LOGGER.error("V1Pod is empty...");
      } else {
        if (pod.getStatus() == null) {
          LOGGER.error("Pod Status is empty");
        } else {
          LOGGER.info("Phase: " + pod.getStatus().getPhase());
        }
      }

      PodLogs logs = new PodLogs();
      InputStream inputStream = logs.streamNamespacedPodLog(pod);

      responseBody = getPodLog(inputStream, pod.getMetadata().getName());
    } catch (ApiException | IOException e) {
      // TODO: handle better throwing so that it can be caught in LogService and the default stream
      // returned rather than failure.
      LOGGER.error("streamPodLog Exception: ", e);
      throw new KubeRuntimeException("Error streamPodLog", e);
    }

    return responseBody;
  }
  
  @Override
  public V1PersistentVolumeClaim createWorkspacePVC(String workspaceName, String workspaceId, String pvcSize) throws ApiException {
    return createPVC(createWorkspaceAnnotations(workspaceName, workspaceId), createWorkspaceLabels(workspaceId), pvcSize);
  }
  
  @Override
  public V1PersistentVolumeClaim createWorkflowPVC(String workflowName, String workflowId,
      String workflowActivityId, String pvcSize) throws ApiException {
    return createPVC(createAnnotations(workflowName, workflowId, workflowActivityId, null), createLabels(workflowId, workflowActivityId, null), pvcSize);
  }

  private V1PersistentVolumeClaim createPVC(Map<String, String> annotations, Map<String, String> labels, String pvcSize) throws ApiException {
    // Setup
    V1PersistentVolumeClaim body = new V1PersistentVolumeClaim();

    // Create Metadata
    V1ObjectMeta metadata = new V1ObjectMeta();
    metadata.annotations(annotations);
    metadata.labels(labels);
    metadata.generateName(getPVCPrefix() + "-");
    body.metadata(metadata);

    // Create PVC Spec
    V1PersistentVolumeClaimSpec pvcSpec = new V1PersistentVolumeClaimSpec();
    List<String> pvcAccessModes = new ArrayList<>();
    pvcAccessModes.add("ReadWriteMany");
    pvcSpec.accessModes(pvcAccessModes);
    V1ResourceRequirements pvcResourceReq = new V1ResourceRequirements();
    Map<String, Quantity> pvcRequests = new HashMap<>();
    if (pvcSize == null || pvcSize.isEmpty()) {
      pvcSize = kubeWorkerPVCSize;
    }
    pvcRequests.put("storage", Quantity.fromString(pvcSize));
    pvcResourceReq.requests(pvcRequests);
    pvcSpec.resources(pvcResourceReq);
    body.spec(pvcSpec);

    V1PersistentVolumeClaim result = getCoreApi().createNamespacedPersistentVolumeClaim(
        kubeNamespace, body, kubeApiIncludeuninitialized, kubeApiPretty, null);
    LOGGER.info(result);
    return result;
  }
  
  @Override
  public V1PersistentVolumeClaimStatus watchWorkspacePVC(String workspaceId) {
    return watchPVC(getWorkspaceLabelSelector(workspaceId));
    
  }
  
  @Override
  public V1PersistentVolumeClaimStatus watchWorkflowPVC(String workflowId, String workflowActivityId) {
    return watchPVC(getLabelSelector(workflowId, workflowActivityId, null));
  }

  private V1PersistentVolumeClaimStatus watchPVC(String labelSelector) {
    V1PersistentVolumeClaimStatus result = null;
    try {
      result =
          getPersistentVolumeClaimStatus(getPersistentVolumeWatch(getCoreApi(), labelSelector));
    } catch (ApiException | IOException e) {
      LOGGER.error("watchPVC Exception: ", e);
      throw new KubeRuntimeException("Error watchPVC", e);
    }
    return result;
  }

  private V1PersistentVolumeClaimStatus getPersistentVolumeClaimStatus(
      Watch<V1PersistentVolumeClaim> watch) throws IOException {
    V1PersistentVolumeClaimStatus result = null;
    try {
      for (Watch.Response<V1PersistentVolumeClaim> item : watch) {
        LOGGER.info(String.format("%s : %s%n", item.type, item.object.getMetadata().getName()));
        LOGGER.info(item.object.getStatus());
        result = item.object.getStatus();
        if (result != null && result.getPhase() != null && "Bound".equals(result.getPhase())) {
          break;
        }
      }

    } finally {
      watch.close();
    }
    return result;
  }

  private Watch<V1PersistentVolumeClaim> getPersistentVolumeWatch(CoreV1Api api,
      String labelSelector) throws ApiException {
    return Watch.createWatch(createWatcherApiClient(),
        api.listNamespacedPersistentVolumeClaimCall(kubeNamespace, kubeApiIncludeuninitialized,
            kubeApiPretty, null, null, labelSelector, null, null, TIMEOUT, true, null, null),
        new TypeToken<Watch.Response<V1PersistentVolumeClaim>>() {}.getType());
  }

  protected String getPVCName(String labelSelector) {
    try {
      V1PersistentVolumeClaimList persistentVolumeClaimList = getCoreApi()
          .listNamespacedPersistentVolumeClaim(kubeNamespace, kubeApiIncludeuninitialized,
              kubeApiPretty, null, null, labelSelector, null, null, TIMEOUT_ONE_MINUTE, false);
      persistentVolumeClaimList.getItems().forEach(pvc -> {
        LOGGER.info(pvc.toString());
        LOGGER.info(" PVC Name: " + pvc.getMetadata().getName());
      });
      if (!persistentVolumeClaimList.getItems().isEmpty()
          && persistentVolumeClaimList.getItems().get(0).getMetadata().getName() != null) {
        LOGGER.info("----- End getPVCName() -----");
        return persistentVolumeClaimList.getItems().get(0).getMetadata().getName();
      }
    } catch (ApiException e) {
      LOGGER.error(EXCEPTION, e);
    }
    return "";
  }
  
  @Override
  public V1Status deleteWorkspacePVC(String workspaceId) {
    return deletePVC(getWorkspaceLabelSelector(workspaceId));
  }
  
  @Override
  public V1Status deleteWorkflowPVC(String workflowId, String workflowActivityId) {
    return deletePVC(getLabelSelector(workflowId, workflowActivityId, null));
  }

  private V1Status deletePVC(String labelSelector) {
    V1DeleteOptions deleteOptions = new V1DeleteOptions();
    // deleteOptions.setPropagationPolicy("Background");
    V1Status result = new V1Status();
    String pvcName = getPVCName(labelSelector);
    if (!pvcName.isEmpty()) {
      LOGGER.info("Deleting PVC (" + pvcName + ")...");
      try {
        result = getCoreApi().deleteNamespacedPersistentVolumeClaim(pvcName, kubeNamespace,
            kubeApiPretty, deleteOptions, null, null, null, null);
      } catch (JsonSyntaxException e) {
        if (e.getCause() instanceof IllegalStateException) {
          IllegalStateException ise = (IllegalStateException) e.getCause();
          if (ise.getMessage() != null
              && ise.getMessage().contains("Expected a string but was BEGIN_OBJECT")) {
            LOGGER.error(
                "Catching exception because of issue https://github.com/kubernetes-client/java/issues/86");
          } else {
            LOGGER.error("Exception when running deletePVC()", e);
          }
        }
      } catch (ApiException e) {
        LOGGER.error("Exception when running deletePVC()", e);
      }
    }
    return result;
  }

  protected String getJobName(boolean onlyOnSuccess, String workflowId, String workflowActivityId,
      String taskId) {
    String labelSelector = getLabelSelector(workflowId, workflowActivityId, taskId);

    try {
      V1JobList listOfJobs =
          getBatchApi().listNamespacedJob(kubeNamespace, kubeApiIncludeuninitialized, kubeApiPretty,
              null, null, labelSelector, null, null, TIMEOUT_ONE_MINUTE, false);
      if (!listOfJobs.getItems().isEmpty()) {
        Optional<V1Job> job = listOfJobs.getItems().stream().filter(
            item -> (onlyOnSuccess && item.getStatus().getSucceeded() != null) || !onlyOnSuccess)
            .findFirst();
        String jobName = job.isPresent() ? job.get().getMetadata().getName() : "";
        LOGGER.info(" Job Name: " + jobName);
        return jobName;
      }
    } catch (ApiException e) {
      LOGGER.error(EXCEPTION, e);
    }
    return "";
  }

  @Override
  public V1Status deleteJob(TaskDeletionEnum taskDeletion, String workflowId, String workflowActivityId,
      String taskId) {
    V1DeleteOptions deleteOptions = new V1DeleteOptions();
    deleteOptions.setPropagationPolicy("Background");
    V1Status result = new V1Status();
    String jobName = getJobName(TaskDeletionEnum.OnSuccess.equals(taskDeletion) ? true : false,
        workflowId, workflowActivityId, taskId);
    if (!jobName.isEmpty()) {
      try {
        LOGGER.info("Deleting Job ( " + jobName + ")...");
        result = getBatchApi().deleteNamespacedJob(jobName, kubeNamespace, kubeApiPretty,
            deleteOptions, null, null, null, null);
      } catch (JsonSyntaxException e) {
        if (e.getCause() instanceof IllegalStateException) {
          IllegalStateException ise = (IllegalStateException) e.getCause();
          if (ise.getMessage() != null
              && ise.getMessage().contains("Expected a string but was BEGIN_OBJECT")) {
            LOGGER.error(
                "Catching exception because of issue https://github.com/kubernetes-client/java/issues/86");
          } else {
            LOGGER.error("Exception when running deleteJob()", e);
          }
        }
      } catch (ApiException e) {
        LOGGER.error("Exception when running deleteJob()", e);
      }
    }
    return result;
  }

  @Override
  public V1ConfigMap createWorkflowConfigMap(String workflowName, String workflowId,
      String workflowActivityId, Map<String, String> inputProps) {
    return createConfigMap(
        createWorkflowConfigMapBody(workflowName, workflowId, workflowActivityId, inputProps));
  }

  @Override
  public V1ConfigMap createTaskConfigMap(String workflowName, String workflowId,
      String workflowActivityId, String taskName, String taskId, Map<String, String> inputProps) {
    LOGGER.info("ConfigMapBody: " + inputProps);
    return createConfigMap(createTaskConfigMapBody(workflowName, workflowId, workflowActivityId,
        taskName, taskId, inputProps));
  }

  private V1ConfigMap createConfigMap(V1ConfigMap body) {
    V1ConfigMap result = new V1ConfigMap();
    try {
      result = getCoreApi().createNamespacedConfigMap(kubeNamespace, body,
          kubeApiIncludeuninitialized, kubeApiPretty, null);
      LOGGER.info(result);
    } catch (ApiException e) {
      LOGGER.error("Exception when calling CoreV1Api#createNamespacedConfigMap", e);
      throw new KubeRuntimeException("Error create configMap", e);
    }
    return result;
  }

  @Override
  public V1ConfigMap watchConfigMap(String workflowId, String workflowActivityId, String taskId) {
    String labelSelector = getLabelSelector(workflowId, workflowActivityId, taskId);

    try {
      Watch<V1ConfigMap> watch = Watch.createWatch(createWatcherApiClient(),
          getCoreApi().listNamespacedConfigMapCall(kubeNamespace, kubeApiIncludeuninitialized,
              kubeApiPretty, null, null, labelSelector, null, null, null, true, null, null),
          new TypeToken<Watch.Response<V1ConfigMap>>() {}.getType());

      return getConfigMapResult(watch);
    } catch (IOException | ApiException e) {
      throw new KubeRuntimeException("Error create configMap watch", e);
    }
  }

  @Override
  public void patchTaskConfigMap(String workflowId, String workflowActivityId, String taskId,
      String taskName, Map<String, String> properties) {
    V1ConfigMap wfConfigMap = getConfigMap(workflowId, workflowActivityId, null);
    String fileName = taskName + ".output.properties";
    patchConfigMap(getConfigMapName(wfConfigMap), fileName,
        getConfigMapDataProp(wfConfigMap, fileName), createConfigMapProp(properties));
  }

  @Override
  public Map<String, String> getTaskOutPutConfigMapData(String workflowId,
      String workflowActivityId, String taskId, String taskName) {
    LOGGER.info("  taskName: " + taskName);
    V1ConfigMap wfConfigMap = getConfigMap(workflowId, workflowActivityId, null);
    String fileName = taskName.replace(" ", "") + ".output.properties";
    String dataString = getConfigMapDataProp(wfConfigMap, fileName);

    Map<String, String> properties =
        PATTERN_PROPERTIES.splitAsStream(dataString.trim()).map(s -> s.split("=", PROPERTY_SIZE))
            .collect(Collectors.toMap(a -> a[0], a -> a.length > 1 ? a[1] : ""));

    LOGGER.info("  properties: " + properties.toString());
    return properties;
  }

  @Override
  public V1Status deleteConfigMap(String workflowId, String workflowActivityId, String taskId) {
    V1DeleteOptions deleteOptions = new V1DeleteOptions();
    V1Status result = new V1Status();

    try {
      String configMapName = getConfigMapName(getConfigMap(workflowId, workflowActivityId, taskId));
      LOGGER.info("Deleting ConfigMap (" + configMapName + ")...");
      result = getCoreApi().deleteNamespacedConfigMap(configMapName, kubeNamespace, kubeApiPretty,
          deleteOptions, null, null, null, null);
    } catch (JsonSyntaxException e) {
      if (e.getCause() instanceof IllegalStateException) {
        IllegalStateException ise = (IllegalStateException) e.getCause();
        if (ise.getMessage() != null
            && ise.getMessage().contains("Expected a string but was BEGIN_OBJECT")) {
          LOGGER.error(
              "Catching exception because of issue https://github.com/kubernetes-client/java/issues/86");
        }

        LOGGER.error("Exception when running deleteConfigMap()", e);
      }
    } catch (ApiException e) {
      LOGGER.error("Exception when running deleteConfigMap()", e);
    }
    return result;
  }
  
  @Override
  public boolean checkWorkspacePVCExists(String workspaceId, boolean failIfNotBound) {
    return checkPVCExists(getWorkspaceLabelSelector(workspaceId), failIfNotBound);
  }

  @Override
  public boolean checkWorkflowPVCExists(String workflowId, String workflowActivityId, String taskId,
      boolean failIfNotBound) {
    return checkPVCExists(getLabelSelector(workflowId, workflowActivityId, taskId), failIfNotBound);
  }
  
  private boolean checkPVCExists(String labelSelector, boolean failIfNotBound) {
    boolean isPVCExists = false;
    try {
      V1PersistentVolumeClaimList persistentVolumeClaimList = getCoreApi()
          .listNamespacedPersistentVolumeClaim(kubeNamespace, kubeApiIncludeuninitialized,
              kubeApiPretty, null, null, labelSelector, null, null, TIMEOUT_ONE_MINUTE, false);
      isPVCExists = isPVCAvailable(failIfNotBound, persistentVolumeClaimList);
    } catch (ApiException e) {
      LOGGER.error(
          "No PVC found matching selector: " + labelSelector, e);
    }
    return isPVCExists;
  }

  protected List<V1EnvVar> createProxyEnvVars() {
    List<V1EnvVar> proxyEnvVars = new ArrayList<>();

    final String proxyUrl = "http://" + proxyHost + ":" + proxyPort;
    proxyEnvVars.add(createEnvVar("PROXY_HOST", proxyHost));
    proxyEnvVars.add(createEnvVar("PROXY_PORT", proxyPort));
    proxyEnvVars.add(createEnvVar("HTTP_PROXY", proxyUrl));
    proxyEnvVars.add(createEnvVar("HTTPS_PROXY", proxyUrl));
    proxyEnvVars.add(createEnvVar("http_proxy", proxyUrl));
    proxyEnvVars.add(createEnvVar("https_proxy", proxyUrl));
    proxyEnvVars.add(createEnvVar("NO_PROXY", proxyIgnore));
    proxyEnvVars.add(createEnvVar("no_proxy", proxyIgnore));
    proxyEnvVars.add(createEnvVar("use_proxy", "on"));

    return proxyEnvVars;
  }

  protected V1EnvVar createEnvVar(String key, String value) {
    V1EnvVar envVar = new V1EnvVar();
    envVar.setName(key);
    envVar.setValue(value);
    return envVar;
  }

  protected String createConfigMapProp(Map<String, String> properties) {
    LOGGER.info("Building ConfigMap Body");
    Properties props = new Properties();
    StringWriter propsSW = new StringWriter();
    if (properties != null && !properties.isEmpty()) {
      properties.forEach((key, value) -> {
        String valueStr = value != null ? value : "";
        LOGGER.info("  " + key + "=" + valueStr);
        props.setProperty(key, valueStr);
      });
    }

    try {
      props.store(propsSW, null);
      LOGGER.info("" + propsSW.toString());
    } catch (IOException ex) {
      LOGGER.error(EXCEPTION, ex);
    }

    return propsSW.toString();
  }

  protected V1ConfigMap getConfigMap(String workflowId, String workflowActivityId, String taskId) {
    V1ConfigMap configMap = null;

    String labelSelector = getLabelSelector(workflowId, workflowActivityId, taskId);
    try {
      V1ConfigMapList configMapList =
          getCoreApi().listNamespacedConfigMap(kubeNamespace, kubeApiIncludeuninitialized,
              kubeApiPretty, null, null, labelSelector, null, null, TIMEOUT_ONE_MINUTE, false);
      if (!configMapList.getItems().isEmpty()) {
        LOGGER.info(" getConfigMap() - Found " + configMapList.getItems().size() + " configmaps: "
            + configMapList.getItems().stream().reduce("", (configMapNames, cm) -> configMapNames +=
                cm.getMetadata().getName() + "(" + cm.getMetadata().getCreationTimestamp() + ")",
                String::concat));
        if (!Optional.ofNullable(taskId).isPresent()) {
          Optional<V1ConfigMap> configMapOptional = configMapList.getItems().stream()
              .filter(cm -> !cm.getMetadata().getLabels().containsKey("task-id")).findFirst();
          configMap = configMapOptional.isPresent() ? configMapOptional.get()
              : configMapList.getItems().get(0);
        } else {
          configMap = configMapList.getItems().get(0);
        }
        LOGGER.info(" getConfigMap() - chosen configmap: " + configMap.getMetadata().getName() + "("
            + configMap.getMetadata().getCreationTimestamp() + ")");
      }
    } catch (ApiException e) {
      LOGGER.error("Error: ", e);
    }
    return configMap;
  }

  protected V1ConfigMap getFirstConfigmapByDateTime(V1ConfigMapList configMapList) {
    V1ConfigMap configMap = null;
    if (!configMapList.getItems().isEmpty()) {
      configMap = configMapList.getItems().get(0);
      DateTime configMapDateTime = configMap.getMetadata().getCreationTimestamp();
      for (int i = 0; i < configMapList.getItems().size(); i++) {
        DateTime configMapDateTimeIter =
            configMapList.getItems().get(i).getMetadata().getCreationTimestamp();
        // if (configMapDateTimeIter != null) &&
        // configMapDateTime.compareTo(configMapDateTimeIter) > 0) {
        if (configMapDateTimeIter != null) {
          LOGGER.info("Comparing " + configMapDateTime + " to " + configMapDateTimeIter + " = "
              + configMapDateTime.compareTo(configMapDateTimeIter));
          if (configMapDateTime.compareTo(configMapDateTimeIter) > 0) {
            configMap = configMapList.getItems().get(i);
            configMapDateTime = configMap.getMetadata().getCreationTimestamp();
          }
        }
      }
    }
    return configMap;
  }

  protected String getConfigMapName(V1ConfigMap configMap) {
    String configMapName = "";

    if (configMap != null && !configMap.getMetadata().getName().isEmpty()) {
      configMapName = configMap.getMetadata().getName();
      LOGGER.info(" ConfigMap Name: " + configMapName);
    }
    return configMapName;
  }

  private V1ConfigMap getConfigMapResult(Watch<V1ConfigMap> watch) throws IOException {
    V1ConfigMap result = null;
    try {
      if (watch.hasNext()) {
        Watch.Response<V1ConfigMap> item = watch.next();
        LOGGER.info(String.format("%s : %s%n", item.type, item.object.getMetadata().getName()));
        result = item.object;
      }
    } finally {
      watch.close();
    }
    return result;
  }

  protected V1VolumeProjection getVolumeProjection(V1ConfigMap wfConfigMap) {
    V1ConfigMapProjection projectedConfigMapTask = new V1ConfigMapProjection();
    projectedConfigMapTask.name(getConfigMapName(wfConfigMap));
    V1VolumeProjection configMapVolSourceTask = new V1VolumeProjection();
    configMapVolSourceTask.configMap(projectedConfigMapTask);
    return configMapVolSourceTask;
  }

  protected V1Volume getVolume(String name) {
    V1Volume workerVolume = new V1Volume();
    workerVolume.name(name);
    return workerVolume;
  }

  protected V1VolumeMount getVolumeMount(String name, String path) {
    V1VolumeMount volMount = new V1VolumeMount();
    volMount.name(name);
    volMount.mountPath(path);
    return volMount;
  }

  protected V1Container getContainer(String image, String command) {
    V1Container container = new V1Container();
    LOGGER.info("Container Image: " + image);
    container.image(image);
    LOGGER.info("Container Command: " + command);
    Optional.ofNullable(command).filter(str -> !str.isEmpty()).ifPresent(str -> container.addCommandItem(str));
    container.name("worker-cntr");
    container.imagePullPolicy(kubeImagePullPolicy);
    V1SecurityContext securityContext = new V1SecurityContext();
    securityContext.setPrivileged(true);
    // Only works with Kube 1.12. ICP 3.1.1 is Kube 1.11.5
    // TODO: securityContext.setProcMount("Unmasked");
    container.setSecurityContext(securityContext);
    return container;
  }

  /*
   * Passes through optional method inputs to the sub methods which need to handle this.
   */
  protected V1ObjectMeta getMetadata(String workflowName, String workflowId,
      String workflowActivityId, String taskId, String generateName) {
    V1ObjectMeta metadata = new V1ObjectMeta();
    metadata.annotations(createAnnotations(workflowName, workflowId, workflowActivityId, taskId));
    metadata.labels(createLabels(workflowId, workflowActivityId, taskId));
    if (StringUtils.isNotBlank(generateName)) {
      metadata.generateName(generateName + "-");
    }
    return metadata;
  }

  /*
   * Sets the tolerations and nodeSelector to match the dedicated node taints and node-role label
   */
  protected void getTolerationAndSelector(V1PodSpec podSpec) {
    V1Toleration nodeTolerationItem = new V1Toleration();
    nodeTolerationItem.key("dedicated");
    nodeTolerationItem.value("bmrg-worker");
    nodeTolerationItem.effect("NoSchedule");
    nodeTolerationItem.operator("Equal");
    podSpec.addTolerationsItem(nodeTolerationItem);
    podSpec.putNodeSelectorItem("node-role.kubernetes.io/bmrg-worker", "true");
  }

  /*
   * Sets the pod anti affinity
   */
  protected void getPodAntiAffinity(V1PodSpec podSpec, Map<String, String> labels) {
    V1LabelSelector labelSelector = new V1LabelSelector();
    labelSelector.setMatchLabels(labels);
    V1PodAffinityTerm podAntiAffinityPreferredTerm = new V1PodAffinityTerm();
    podAntiAffinityPreferredTerm.setLabelSelector(labelSelector);
    podAntiAffinityPreferredTerm.setTopologyKey("kubernetes.io/hostname");
    V1WeightedPodAffinityTerm podAntiAffinityPreferred = new V1WeightedPodAffinityTerm();
    podAntiAffinityPreferred.setWeight(100);
    podAntiAffinityPreferred.setPodAffinityTerm(podAntiAffinityPreferredTerm);
    V1PodAntiAffinity podAntiAffinity = new V1PodAntiAffinity();
    podAntiAffinity
        .addPreferredDuringSchedulingIgnoredDuringExecutionItem(podAntiAffinityPreferred);
    V1Affinity podAffinity = new V1Affinity();
    podAffinity.setPodAntiAffinity(podAntiAffinity);
    podSpec.affinity(podAffinity);
  }

  private boolean isPVCAvailable(boolean failIfNotBound,
      V1PersistentVolumeClaimList persistentVolumeClaimList) {
    if (!persistentVolumeClaimList.getItems().isEmpty()) {
      persistentVolumeClaimList.getItems().forEach(pvc -> LOGGER
          .info("PVC: " + pvc.getMetadata().getName() + " (" + pvc.getStatus().getPhase() + ")"));
      if (failIfNotBound) {
        if (persistentVolumeClaimList.getItems().stream()
            .filter(pvc -> "Bound".equalsIgnoreCase(pvc.getStatus().getPhase())).count() > 0) {
          // TODO update to check if they are terminating (even though they are still bound)
          return true;
        }
      } else {
        return true;
      }
    }

    return false;
  }

  private ApiClient createWatcherApiClient() {
    // This leverages the default ApiClient in the KubeConfiguration.java Class and overrides the
    // debugging to false as Watch is (for now) incompatible with debugging mode active.
    // Watches will not return data until the watch connection terminates
    // io.kubernetes.client.ApiException: Watch is incompatible with debugging mode active.
    ApiClient watcherClient =
        io.kubernetes.client.Configuration.getDefaultApiClient().setDebugging(false);
    watcherClient.getHttpClient().setReadTimeout(kubeApiTimeOut.longValue(), TimeUnit.SECONDS);
    return watcherClient;

  }

  protected Watch<V1Job> createJobWatch(BatchV1Api api, String labelSelector) throws ApiException {
    return Watch.createWatch(createWatcherApiClient(),
        api.listNamespacedJobCall(kubeNamespace, kubeApiIncludeuninitialized, kubeApiPretty, null,
            null, labelSelector, null, null, null, true, null, null),
        new TypeToken<Watch.Response<V1Job>>() {}.getType());
  }

  protected Watch<V1Pod> createPodWatch(String labelSelector, CoreV1Api api) throws ApiException {
    return Watch.createWatch(createWatcherApiClient(),
        api.listNamespacedPodCall(kubeNamespace, kubeApiIncludeuninitialized, kubeApiPretty, null,
            null, labelSelector, null, null, null, true, null, null),
        new TypeToken<Watch.Response<V1Pod>>() {}.getType());
  }


  protected V1Job getJobResult(String taskId, Watch<V1Job> watch) throws IOException {
    V1Job jobResult = null;
    try {
      jobResult = getJob(taskId, watch);
    } finally {
      if (watch != null) {
        watch.close();
      }
    }
    return jobResult;
  }

  private V1Job getJob(String taskId, Watch<V1Job> watch) {
    V1Job jobResult = null;
    for (Watch.Response<V1Job> item : watch) {

      LOGGER.info(item.type + " : " + item.object.getMetadata().getName());
      LOGGER.info(item.object.getStatus());
      if (item.object.getStatus().getSucceeded() != null
          && item.object.getStatus().getSucceeded() >= 1) {
        LOGGER.info(String.format("Task (%s) has succeeded.", taskId));
        jobResult = item.object;
        break;
      } else if (item.object.getStatus().getFailed() != null
          && item.object.getStatus().getFailed() >= kubeWorkerJobBackOffLimit) {
        throw new KubeRuntimeException("Task (" + taskId + ") has failed to execute "
            + kubeWorkerJobBackOffLimit + " times triggering failure.");
      }
    }

    return jobResult;
  }

  private String getConfigMapDataProp(V1ConfigMap configMap, String key) {
    String configMapDataProp = "";

    if (configMap.getData().get(key) != null) {
      configMapDataProp = configMap.getData().get(key);
      LOGGER.info(" ConfigMap Input Properties Data: " + configMapDataProp);
    }
    return configMapDataProp;
  }

  private void patchConfigMap(String name, String dataKey, String origData, String newData) {
    JsonObject jsonPatchObj = new JsonObject();
    jsonPatchObj.addProperty("op", "add");
    jsonPatchObj.addProperty("path", "/data/" + dataKey);
    jsonPatchObj.addProperty("value",
        origData.endsWith("\n") ? (origData + newData) : (origData + "\n" + newData));

    ArrayList<JsonObject> arr = new ArrayList<>();
    arr.add(((new Gson()).fromJson(jsonPatchObj.toString(), JsonElement.class)).getAsJsonObject());
    try {
      V1ConfigMap result =
          getCoreApi().patchNamespacedConfigMap(name, kubeNamespace, arr, kubeApiPretty, null);
      LOGGER.info(result);
    } catch (ApiException e) {
      LOGGER.error("Exception when calling CoreV1Api#patchNamespacedConfigMap", e);
    }
  }

  protected StreamingResponseBody getPodLog(InputStream inputStream, String podName) {
    return outputStream -> {
      byte[] data = new byte[BYTE_SIZE];
      int nRead = 0;
      int nReadSum = 0;
      LOGGER.info("Log stream started for pod " + podName + "...");
      try {
        while ((nRead = inputStream.read(data)) > 0) {
          outputStream.write(data, 0, nRead);
          nReadSum += nRead;
        }
      } finally {
        outputStream.flush();
        LOGGER.info("Log stream completed for pod " + podName + ", total bytes streamed=" + nReadSum
            + "...");
        inputStream.close();
        LOGGER.info("Log stream closed for pod " + podName + "...");
      }
    };
  }

  protected V1Pod getPod(Watch<V1Pod> watch) throws IOException {
    V1Pod pod = null;
    try {
      for (Watch.Response<V1Pod> item : watch) {

        String name = item.object.getMetadata().getName();
        LOGGER.info("Pod: " + name + "...");
        LOGGER.info("Pod Start Time: " + item.object.getStatus().getStartTime() + "...");
        String phase = item.object.getStatus().getPhase();
        LOGGER.info("Pod Phase: " + phase + "...");
        for (V1PodCondition condition : item.object.getStatus().getConditions()) {
          LOGGER.info("Pod Condition: " + condition.toString() + "...");
        }
        for (V1ContainerStatus containerStatus : item.object.getStatus().getContainerStatuses()) {
          LOGGER.info("Container Status: " + containerStatus.toString() + "...");
        }

        if (!("pending".equalsIgnoreCase(phase) || "unknown".equalsIgnoreCase(phase))) {
          LOGGER.info("Pod " + name + " ready to stream logs...");
          pod = item.object;
          break;
        }
      }
    } finally {
      watch.close();
    }
    return pod;
  }

  protected CoreV1Api getCoreApi() {
    return apiClient == null ? new CoreV1Api() : new CoreV1Api(apiClient);
  }

  protected BatchV1Api getBatchApi() {
    return apiClient == null ? new BatchV1Api() : new BatchV1Api(apiClient);
  }

  void setApiClient(ApiClient apiClient) {
    this.apiClient = apiClient;
  }

  public ApiClient getApiClient() {
    return this.apiClient;
  }

  protected String getTaskDebug(TaskConfiguration taskConfiguration) {
    return taskConfiguration.getDebug() != null ? taskConfiguration.getDebug().toString()
        : configurationService.getTaskDebug().toString();
  }
  
  protected Map<String, String> createWorkspaceLabels(String workspaceId) {
    Map<String, String> labels = new HashMap<>();
    labels.put("app.kubernetes.io/name", "worker");
    labels.put("app.kubernetes.io/instance", "worker-"+workspaceId);
    labels.put("app.kubernetes.io/part-of", "bmrg-flow");
    labels.put("app.kubernetes.io/managed-by", "controller");
    labels.put("boomerang.io/product", "bmrg-flow");
    labels.put("boomerang.io/tier", "worker");
    labels.put("boomerang.io/workspace-id", workspaceId);
    return labels;
  }
  
  protected Map<String, String> createWorkspaceAnnotations(String workspaceName, String workspaceId) {
    Map<String, String> annotations = new HashMap<>();
    annotations.put("boomerang.io/workspace-name", workspaceName);
    annotations.put("boomerang.io/selector", getWorkspaceLabelSelector(workspaceId));
    return annotations;
  }
  
  protected String getWorkspaceLabelSelector(String workspaceId) {
    StringBuilder labelSelector = new StringBuilder("boomerang.io/product=bmrg-flow,boomerang.io/tier=worker,boomerang.io/workspace-id=" + workspaceId);

    LOGGER.info("  labelSelector: " + labelSelector.toString());
    return labelSelector.toString();
  }
}
