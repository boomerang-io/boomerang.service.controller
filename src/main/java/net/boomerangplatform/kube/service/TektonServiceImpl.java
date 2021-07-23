package net.boomerangplatform.kube.service;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.fabric8.knative.internal.pkg.apis.Condition;
import io.fabric8.kubernetes.api.model.ConfigMapProjection;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource;
import io.fabric8.kubernetes.api.model.ProjectedVolumeSource;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeProjection;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.tekton.client.DefaultTektonClient;
import io.fabric8.tekton.client.TektonClient;
import io.fabric8.tekton.pipeline.v1beta1.ArrayOrString;
import io.fabric8.tekton.pipeline.v1beta1.Param;
import io.fabric8.tekton.pipeline.v1beta1.ParamSpec;
import io.fabric8.tekton.pipeline.v1beta1.Step;
import io.fabric8.tekton.pipeline.v1beta1.TaskRun;
import io.fabric8.tekton.pipeline.v1beta1.TaskRunBuilder;
import io.fabric8.tekton.pipeline.v1beta1.TaskRunResult;
import io.fabric8.tekton.pipeline.v1beta1.WorkspaceBinding;
import io.fabric8.tekton.pipeline.v1beta1.WorkspaceDeclaration;
import net.boomerangplatform.error.BoomerangError;
import net.boomerangplatform.error.BoomerangException;
import net.boomerangplatform.model.TaskConfiguration;
import net.boomerangplatform.model.TaskEnvVar;
import net.boomerangplatform.model.TaskResponseResultParameter;
import net.boomerangplatform.model.TaskResultParameter;

@Component
public class TektonServiceImpl {

  private static final Logger LOGGER = LogManager.getLogger(TektonServiceImpl.class);

  @Autowired
  protected NewHelperKubeServiceImpl helperKubeService;
  
  @Autowired
  protected NewKubeServiceImpl kubeService;
    
  protected static final Integer ONE_DAY_IN_SECONDS = 86400; // 60*60*24

  @Value("${kube.image.pullPolicy}")
  protected String kubeImagePullPolicy;

  @Value("${kube.image.pullSecret}")
  protected String kubeImagePullSecret;
  
  @Value("${kube.lifecycle.image}")
  protected String kubeLifecycleImage;

  @Value("${kube.worker.job.backOffLimit}")
  protected Integer kubeJobBackOffLimit;

  @Value("${kube.worker.job.restartPolicy}")
  protected String kubeJobRestartPolicy;
    
  @Value("${kube.worker.job.ttlDays}")
  protected Integer kubeJobTTLDays;

  @Value("${kube.worker.serviceaccount}")
  protected String kubeJobServiceAccount;

  @Value("${kube.resource.limit.ephemeral-storage}")
  private String kubeResourceLimitEphemeralStorage;

  @Value("${kube.resource.request.ephemeral-storage}")
  private String kubeResourceRequestEphemeralStorage;

  @Value("${kube.resource.limit.memory}")
  private String kubeResourceLimitMemory;

  @Value("${kube.resource.request.memory}")
  private String kubeResourceRequestMemory;

  @Value("${kube.worker.storage.data.memory}")
  private Boolean kubeWorkerStorageDataMemory;

  @Value("${kube.worker.node.dedicated}")
  protected Boolean kubeJobDedicatedNodes;

  @Value("${kube.worker.hostaliases}")
  protected String kubeHostAliases;

  TektonClient client = null;

  public TektonServiceImpl() {
    this.client = new DefaultTektonClient();
  }
  
  public TaskRun createTaskRun(String workspaceId, String workflowName,
      String workflowId, String workflowActivityId, String taskActivityId, String taskName,
      String taskId, Map<String, String> customLabels, String image, List<String> command, List<String> arguments,
      Map<String, String> parameters, List<TaskEnvVar> envVars, List<TaskResultParameter> results, String workingDir, 
      TaskConfiguration configuration, String script, long waitSeconds) throws InterruptedException {

    LOGGER.info("Initializing Task...");
    
    /*
     * Define environment variables made up of
     * TODO: securityContext.setProcMount("Unmasked");
     *  - Only works with Kube 1.12 and above
     * TODO: determine if we need to do no network as an option
     */
//    SecurityContext securityContext = new SecurityContext();
//    securityContext.setPrivileged(true);
    
    /*
     * Create a resource request and limit for ephemeral-storage Defaults to application.properties,
     * can be overridden by user property.
     * Create a resource request and limit for memory Defaults to application.properties, can be
     * overridden by user property. Maximum of 32Gi.
     */
//    ResourceRequirements resources = new ResourceRequirements();
//    Map<String, Quantity> resourceRequests = new HashMap<>();
//    resourceRequests.put("ephemeral-storage", new Quantity(kubeResourceRequestEphemeralStorage));
//    resourceRequests.put("memory", new Quantity(kubeResourceRequestMemory));
//    Map<String, Quantity> resourceLimits = new HashMap<>();
//    resourceLimits.put("ephemeral-storage", new Quantity(kubeResourceLimitEphemeralStorage));
//    String kubeResourceLimitMemoryQuantity = taskProperties.get("worker.resource.memory.size");
//    if (kubeResourceLimitMemoryQuantity != null && !(Integer.valueOf(kubeResourceLimitMemoryQuantity.replace("Gi", "")) > 32)) {
//      LOGGER.info("Setting Resource Memory Limit to " + kubeResourceLimitMemoryQuantity + "...");
//      resourceLimits.put("memory", new Quantity(kubeResourceLimitMemoryQuantity));
//    } else {
//      LOGGER
//          .info("Setting Resource Memory Limit to default of: " + kubeResourceLimitMemory + " ...");
//      resourceLimits.put("memory", new Quantity(kubeResourceLimitMemory));
//    }
//    resources.setLimits(resourceLimits);
    
    /*
     * Create volumes and Volume Mounts
     * - /workspace for cross workflow persistence such as caches (optional if mounted prior) 
     * - /workflow for workflow based sharing between tasks (optional if mounted prior) 
     * - /props for mounting config_maps 
     * - /data for task storage (optional - needed if using in memory storage)
     */
    List<VolumeMount> volumeMounts = new ArrayList<>();
    List<Volume> volumes = new ArrayList<>();
    if (workspaceId != null && !workspaceId.isEmpty() && kubeService.checkWorkspacePVCExists(workspaceId, false)) {
      VolumeMount wsVolumeMount = new VolumeMount();
      wsVolumeMount.setName(helperKubeService.getPrefixVol() + "-ws");
      wsVolumeMount.setMountPath("/workspace");
      volumeMounts.add(wsVolumeMount);

      Volume wsVolume = new Volume();
      wsVolume.setName(helperKubeService.getPrefixVol() + "-ws");
      PersistentVolumeClaimVolumeSource wsPVCVolumeSource = new PersistentVolumeClaimVolumeSource();
      wsPVCVolumeSource.setClaimName(kubeService.getPVCName(helperKubeService.getWorkspaceLabels(workspaceId, customLabels)));
      wsVolume.setPersistentVolumeClaim(wsPVCVolumeSource);
      volumes.add(wsVolume);
    }
    
    List<WorkspaceDeclaration> taskSpecWorkspaces = new ArrayList<>();
    List<WorkspaceBinding> taskWorkspaces = new ArrayList<>();
    //TODO: determine if optional=true works better than checking if the PVC exists
    if (!kubeService.getPVCName(helperKubeService.getWorkflowLabels(workflowId, workflowActivityId, customLabels)).isEmpty()) {
//      VolumeMount wfVolumeMount = new VolumeMount();
//      wfVolumeMount.setName(helperKubeService.getPrefixVol() + "-wf");
//      wfVolumeMount.setMountPath("/workflow");
//      volumeMounts.add(wfVolumeMount);
      WorkspaceDeclaration wfWorkspaceDeclaration = new WorkspaceDeclaration();
      wfWorkspaceDeclaration.setName(helperKubeService.getPrefixVol() + "-wf");
      wfWorkspaceDeclaration.setMountPath("/workflow");
      wfWorkspaceDeclaration.setDescription("Storage for the specific workflow execution");
      taskSpecWorkspaces.add(wfWorkspaceDeclaration);
      
//      Volume wfVolume = new Volume();
//      wfVolume.setName(helperKubeService.getPrefixVol() + "-wf");
      PersistentVolumeClaimVolumeSource wfPVCVolumeSource = new PersistentVolumeClaimVolumeSource();
      wfPVCVolumeSource.setClaimName(kubeService.getPVCName(helperKubeService.getWorkflowLabels(workflowId, workflowActivityId, customLabels)));
//      wfVolume.setPersistentVolumeClaim(wfPVCVolumeSource);
//      volumes.add(wfVolume);
      
      WorkspaceBinding wfWorkspaceBinding = new WorkspaceBinding();
      wfWorkspaceBinding.setName(helperKubeService.getPrefixVol() + "-wf");
      wfWorkspaceBinding.setPersistentVolumeClaim(wfPVCVolumeSource);
      taskWorkspaces.add(wfWorkspaceBinding);
    }
    
    /*
     * The following code is integrated to the helm chart and CICD properties It allows for
     * containers that breach the standard ephemeral-storage size by off-loading to memory See:
     * https://kubernetes.io/docs/concepts/storage/volumes/#emptydir
     */
    VolumeMount dataVolumeMount = new VolumeMount();
    dataVolumeMount.setName(helperKubeService.getPrefixVol() + "-data");
    dataVolumeMount.setMountPath("/data");
    volumeMounts.add(dataVolumeMount);
    
    Volume dataVolume = new Volume();
    dataVolume.setName(helperKubeService.getPrefixVol() + "-data");
    EmptyDirVolumeSource dataEmptyDirVolumeSource = new EmptyDirVolumeSource();
    if (kubeWorkerStorageDataMemory
        && Boolean.valueOf(parameters.get("worker.storage.data.memory"))) {
      LOGGER.info("Setting /data to in memory storage...");
      dataEmptyDirVolumeSource.setMedium("Memory");
    }
    dataVolume.setEmptyDir(dataEmptyDirVolumeSource);
    volumes.add(dataVolume);

    /*
     * Creation of property projected volume to mount workflow and task configmaps
     */
    VolumeMount propsVolumeMount = new VolumeMount();
    propsVolumeMount.setName(helperKubeService.getPrefixVol() + "-props");
    propsVolumeMount.setMountPath("/props");
    volumeMounts.add(propsVolumeMount);
    
    Volume propsVolume = new Volume();
    propsVolume.setName(helperKubeService.getPrefixVol() + "-props");
    ProjectedVolumeSource projectedVolPropsSource = new ProjectedVolumeSource();
    List<VolumeProjection> projectPropsVolumeList = new ArrayList<>();
    VolumeProjection taskCMVolumeProjection = new VolumeProjection();
    ConfigMapProjection projectedTaskConfigMap = new ConfigMapProjection();
    projectedTaskConfigMap.setName(kubeService.getConfigMapName(helperKubeService.getTaskLabels( workflowId, workflowActivityId, taskId, taskActivityId, customLabels)));
    taskCMVolumeProjection.setConfigMap(projectedTaskConfigMap);    
    projectPropsVolumeList.add(taskCMVolumeProjection);
    projectedVolPropsSource.setSources(projectPropsVolumeList);
    propsVolume.setProjected(projectedVolPropsSource);
    volumes.add(propsVolume);
       
    /*
     * Configure Node Selector and Tolerations if defined
     */
    List<Toleration> tolerations = new ArrayList<>();
    Map<String, String> nodeSelectors = new HashMap<>();
    if (kubeJobDedicatedNodes) {
      Toleration toleration = new Toleration();
      toleration.setKey("dedicated");
      toleration.setValue("bmrg-worker");
      toleration.setEffect("NoSchedule");
      toleration.setOperator("Equal");
      tolerations.add(toleration);
      nodeSelectors.put("node-role.kubernetes.io/bmrg-worker", "true");
    }
    
    /*
     * Create Host Aliases if defined
     */
    List<HostAlias> hostAliases = new ArrayList<>();
    if (!kubeHostAliases.isEmpty()) {
      Type listHostAliasType = new TypeToken<List<HostAlias>>() {}.getType();
      hostAliases = new Gson().fromJson(kubeHostAliases, listHostAliasType);
    }
    
    /*
     * Define Image Pull Secrets
     */
    LocalObjectReference imagePullSecret = new LocalObjectReference();
    imagePullSecret.setName(kubeImagePullSecret);
    List<LocalObjectReference> imagePullSecrets = new ArrayList<>();
    imagePullSecrets.add(imagePullSecret);
    
    /*
     * Define environment variables made up of
     * - Proxy (if enabled)
     * - Boomerang Flow
     * - Debug and CI
     * - Task defined
     */
    List<EnvVar> tknEnvVars = new ArrayList<>();
    tknEnvVars.addAll(helperKubeService.createProxyEnvVars());
    tknEnvVars.addAll(helperKubeService.createEnvVars(workflowId, workflowActivityId, taskName, taskId, taskActivityId));
    tknEnvVars.add(helperKubeService.createEnvVar("DEBUG", helperKubeService.getTaskDebug(configuration)));
    tknEnvVars.add(helperKubeService.createEnvVar("CI", "true"));
    if (envVars != null) {
      envVars.forEach(var -> {
        tknEnvVars.add(helperKubeService.createEnvVar(var.getName(), var.getValue()));
      });
    }
    
    /*
     * Define Task Params and Task Spec Params
     * Additionally default an environment variable for the Task Param prefixed with PARAM_
     */
    List<ParamSpec> taskSpecParams = new ArrayList<>();
    List<Param> taskParams = new ArrayList<>();
    parameters.forEach((key, value) -> {
      ParamSpec taskSpecParam = new ParamSpec();
      taskSpecParam.setName(key);
      taskSpecParam.setType("string");
      taskSpecParams.add(taskSpecParam);
      Param taskParam = new Param();
      taskParam.setName(key);
      ArrayOrString valueString = new ArrayOrString();
      valueString.setStringVal(value);
      taskParam.setValue(valueString);
      taskParams.add(taskParam);
//      Determine if we need this. For now we keep the configmap.
//      envVars.add(helperKubeService.createEnvVar("PARAM_" + key.toUpperCase(), value));
    });
    
    /*
     * Create the main task container
     * Notes:
     *  - If script != null or empty then don't add command (Ref: https://github.com/tektoncd/pipeline/blob/main/docs/tasks.md#running-scripts-within-steps)
     */
    List<Step> taskSteps = new ArrayList<>();
    Step taskStep = new Step();
    taskStep.setName("task");
    taskStep.setImage(image);
    if (script != null && !script.isEmpty()) {
      taskStep.setScript(script);
    }
    taskStep.setCommand(command);
    taskStep.setImagePullPolicy(kubeImagePullPolicy);
    taskStep.setArgs(arguments);
    taskStep.setEnv(tknEnvVars);
    taskStep.setVolumeMounts(volumeMounts);
    taskStep.setWorkingDir(workingDir);
//    taskStep.setSecurityContext(securityContext);
//    taskContainer.setResources(resources);
    taskSteps.add(taskStep);
    
    /*
     * Create the additional PodTemplate based controls
     * TODO: figure out if volumes go here or on the TaskRunSpec
     */
//    Template taskPodTemplate = new Template();
//    taskPodTemplate.setNodeSelector(nodeSelectors);
//    taskPodTemplate.setTolerations(tolerations);
//    taskPodTemplate.setImagePullSecrets(imagePullSecrets);
//    taskPodTemplate.setHostAliases(hostAliases);
////    taskPodTemplate.setVolumes(volumes);
//    
//    LOGGER.info(taskPodTemplate);
    
    /*
     * Define TaskResults and copy from internal model
     */
    List<io.fabric8.tekton.pipeline.v1beta1.TaskResult> tknTaskResults = new ArrayList<>();
    if (results != null) {
      results.forEach(result -> {
        tknTaskResults.add(new io.fabric8.tekton.pipeline.v1beta1.TaskResult(result.getDescription(), result.getName()));
      });
    }    
    
    /*
     * Build out TaskRun definition.
     * - Optionally loads Node Selector and Tolerations
     * TODO: determine how to make Task Workspace work. Currently if using the local-path (bound to a particular node)
     * the Task doesn't find a node to allow it to run. It cant handle the standard Pod method of determine if all its
     * volumes can be satisfied
     * Notes:
     * - Parameters passed into the TaskRun MUST be parameters on the task
     */
    TaskRun taskRun = new TaskRunBuilder()
      .withNewMetadata()
      .withGenerateName(helperKubeService.getPrefixTask() + "-" + taskActivityId + "-")
      .withLabels(helperKubeService.getTaskLabels(workflowId, workflowActivityId, taskId, taskActivityId, customLabels))
      .withAnnotations(helperKubeService.getAnnotations("task", workflowName, workflowId,
          workflowActivityId, taskId, taskActivityId))
      .endMetadata()
      .withNewSpec()
//      .withPodTemplate(taskPodTemplate) 
      .withNewPodTemplate()
      .addToNodeSelector(nodeSelectors)
      .addAllToTolerations(tolerations)
      .addAllToImagePullSecrets(imagePullSecrets)
      .addAllToHostAliases(hostAliases)
      .endPodTemplate()
      .withParams(taskParams)
      .withWorkspaces(taskWorkspaces)
      .withNewTaskSpec()
      .withWorkspaces(taskSpecWorkspaces)
      .withParams(taskSpecParams)
      .withResults(tknTaskResults)
      .withVolumes(volumes)
      .withSteps(taskSteps)
      .endTaskSpec()
//      .addNewWorkspace()
//      .withName("flow")
//      .withNewPersistentVolumeClaim(kubeService.getPVCName(helperKubeService.getWorkspaceLabels(workspaceId, customLabels)), false)
//      .endWorkspace()
      .endSpec()
      .build();
    
    LOGGER.info(taskRun);
    
    TaskRun result = client.v1beta1().taskRuns().create(taskRun);
    
//    client.batch().jobs().withLabels(helperKubeService.getTaskLabels(workflowId, workflowActivityId, taskId, taskActivityId, customLabels)).waitUntilReady(waitSeconds, TimeUnit.SECONDS);

    return result;
  }
  
  public List<TaskResponseResultParameter> watchTask(String workflowId, String workflowActivityId, String taskId,
      String taskActivityId, Map<String, String> customLabels, Integer timeout) throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);
    Condition condition = null;
    List<TaskRunResult> tknResultParameters = new ArrayList<TaskRunResult>();
    
    TaskWatcher taskWatcher = new TaskWatcher(latch);

    try (Watch ignore = client
        .v1beta1().taskRuns().withLabels(helperKubeService.getTaskLabels(workflowId, workflowActivityId, taskId, taskActivityId, customLabels))
        .watch(taskWatcher)) {
      
      //TODO is there a way to wait 3 minutes and check if the task
      //has moved from initial state. If its still in initial state then check PVC
      //PVC might have Event / Condition "ProvisioningFailed" with a reason.
      
      boolean taskComplete = latch.await(timeout, TimeUnit.MINUTES);
      if (!taskComplete) {
        // TODO: implement the TaskRuns timeout and leave this here as a final catch.
        throw new BoomerangException(BoomerangError.TASK_EXECUTION_ERROR, "TaskRunTimeout - Task timed out while waiting for completion.");
      }
      
      condition = taskWatcher.getCondition();
      tknResultParameters = taskWatcher.getResults();
      
      if (condition != null && "True".equals(condition.getStatus())) {
        LOGGER.info("Task completed successfully");
      } else {
        LOGGER.info("Task execution error. " + condition.getReason() + " - " + condition.getMessage());
        if (kubeService.isTaskRunResultTooLarge(helperKubeService.getTaskLabels(workflowId, workflowActivityId, taskId, taskActivityId, customLabels))) {
          throw new BoomerangException(BoomerangError.TASK_EXECUTION_ERROR, "TaskRunResultTooLarge - Task has exceeded the maximum allowed 4096 byte size for Result Parameters.");
        } else {
          throw new BoomerangException(BoomerangError.TASK_EXECUTION_ERROR, condition.getReason() + " - " + condition.getMessage());
        }
      }
      
    } catch (Exception e) {
      LOGGER.error(e.toString());
      throw e;
    }
    
    List<TaskResponseResultParameter> resultParameters = new ArrayList<>();
    tknResultParameters.forEach(tknResult -> {
      resultParameters.add(new TaskResponseResultParameter(tknResult.getName(), tknResult.getValue()));
    });
    
    return resultParameters;
  }
  
  public void deleteTask(String workflowId,
      String workflowActivityId, String taskId, String taskActivityId, Map<String, String> customLabels) {

    LOGGER.debug("Deleting Task...");
    
    client.v1beta1().taskRuns().withLabels(helperKubeService.getTaskLabels(workflowId, workflowActivityId, taskId, taskActivityId, customLabels)).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
  }
  
  /*
   * Cancel a TaskRun
   * 
   * The implementation needs to replace old conditions with the single status condition to be added.
   * Without this, you will receive back a "Not all Steps in the Task have finished executing" message
   * 
   * Reference(s):
   * - https://github.com/abayer/tektoncd-pipeline/blob/0.8.0-jx-support-backwards-incompats/pkg/reconciler/taskrun/cancel.go
   */
  public void cancelTask(String workflowId, String workflowActivityId, String taskId, String taskActivityId, Map<String, String> customLabels) {
    Map<String, String> labels = helperKubeService.getTaskLabels(workflowId, workflowActivityId, taskId, taskActivityId, customLabels);
  
    LOGGER.info("Cancelling Task with labels: " + labels.toString());
    
    List<TaskRun> taskRuns = client.v1beta1().taskRuns().withLabels(labels).list().getItems();
    
    if (taskRuns != null && !taskRuns.isEmpty()) {
      TaskRun taskRun = taskRuns.get(0);
      
      List<Condition> taskRunConditions = new ArrayList<>();
      Condition taskRunCancelCondition = new Condition();
      taskRunCancelCondition.setType("Succeeded");
      taskRunCancelCondition.setStatus("False");
      taskRunCancelCondition.setReason("TaskRunCancelled");
      taskRunCancelCondition.setMessage("The TaskRun was cancelled successfully.");
      taskRunConditions.add(taskRunCancelCondition);
      
      taskRun.getStatus().setConditions(taskRunConditions);
  
      client.v1beta1().taskRuns().updateStatus(taskRun);
    } else if (taskRuns != null && taskRuns.isEmpty()) {
      throw new BoomerangException(BoomerangError.TASK_EXECUTION_ERROR, "CANCEL_FAILURE - No tasks found matching the lables: " + labels.toString());
    } else {
      throw new BoomerangException(BoomerangError.TASK_EXECUTION_ERROR, "CANCEL_FAILURE - Unknown error attempting to cancel task.");
    }
  }
}
