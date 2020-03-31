package org.summerchill;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import lombok.extern.flogger.Flogger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.summerchill.utils.AzkabanUtil;
import org.summerchill.utils.Constant;
import org.summerchill.utils.HttpUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * 对外提供的入口类,他们用来获取Azkanban指定调度中的任务依赖.
 * 提供参数: 第一个参数是指定的文件路径(文件中一行一个任务) 第二个参数是 指定的调度名称. 第三个参数是对应的关系类型(Parents,Children,Ancestors,Descendants)
 */
@Slf4j
public class MainClass {
    public static String sessionId = "";
    public static Map<String, HashSet<String>> jobParentMap = new HashMap<>();
    public static Map<String, HashSet<String>> jobChildrenMap = new HashMap<>();


    public static void main(String[] args) throws IOException {
        checkValidateParams(args);
        //把文件中的每一行存放到一个List中.
        String filePath = args[0];
        String projectName = args[1];
        String realtionShip = args[2];

        getSpecifyJobsDependency(filePath,projectName,projectName,realtionShip);
    }

    /**
     * 检查传来的参数是否合法
     * 第一个参数是文件的绝对路径,看路径对应的文件是否存在.
     * 第二个参数是 指定的调度名称. 目前的调度和
     * 第三个参数是对应的关系类型(Parents,Children,Ancestors,Descendants)
     * @param args
     */
    private static void checkValidateParams(String[] args) {
        String filePath = "";
        String relationShip = "";
        //首先检查个数
        if(args.length == 3){
            filePath = args[0];
            relationShip = args[2];
            File file = new File(filePath);
            if(!file.exists()){
                log.error("您指定的任务列表文件不存在,请检查对应文件路径!");
                System.exit(-1);
            }
            //如果输入的关系不是所预定义的,
            if(!Constant.PARENTS.equalsIgnoreCase(relationShip) && !Constant.CHILDREN.equalsIgnoreCase(relationShip)
            && !Constant.ANCESTORS.equalsIgnoreCase(relationShip) &&  !Constant.DESCENDANTS.equalsIgnoreCase(relationShip)){
                log.error("您指定的关系非PARENTS,CHILDREN,DESCENDANTS,ANCESTORS中的其中一种,请检查!");
                System.exit(-1);
            }
        }else{
            log.error("输入的参数格式非法,第一个参数为任务列表文件绝对路径,第二个参数是对应的调度名称,第三个参数是所要的任务关系!");
            System.exit(-1);
        }
    }

    /**
     * 该方法用于获取指定Job的指定的依赖关系, 以后修改就不需要修改main方法了,只修改该方法即可.
     */
    private static void getSpecifyJobsDependency(String filePath, String projectName, String flowName, String relationShip) throws IOException {
        File jobListFile = new File(filePath);
        String fileInDirect = jobListFile.getParent();
        String jobListFileNameNoExtend = FilenameUtils.removeExtension(jobListFile.getName());
        String resultFileName = jobListFileNameNoExtend + "_" + relationShip + ".txt";
        List<String> jobList = Files.readLines(jobListFile, Charsets.UTF_8);

        //登录Azkaban
        getSessionId("admin", "*****");

        //目前龙湖的使用是,flowName 和 projectName是相同的.
        //初始化一遍对应项目的依赖关系.
        initJobDependcy(projectName, flowName);

        StringBuilder resultSb = new StringBuilder();
        String resultJobListStr = "";
        for (String job : jobList) {
            if(!Strings.isNullOrEmpty(job)){
                //job中可能含有后缀名.
                job = FilenameUtils.removeExtension(job.trim());
                resultSb.append("job:" + job + "的所有" + relationShip + "如下:" + "\n");
                if(Constant.DESCENDANTS.equalsIgnoreCase(relationShip)){
                    resultJobListStr = getJobDescendents(job);
                } else if(Constant.PARENTS.equalsIgnoreCase(relationShip)){
                    resultJobListStr = getJobParents(job);
                } else if(Constant.CHILDREN.equalsIgnoreCase(relationShip)){
                    resultJobListStr = getJobChildren(job);
                } else{
                    resultJobListStr = getJobAncestors(job);
                }
                resultSb.append(resultJobListStr);
                resultSb.append("\n");
            }
        }
        //把azkabanSb写入文件
        try (PrintWriter out = new PrintWriter(fileInDirect + "/" + resultFileName)) {
            out.println(resultSb.toString());
            log.info("结果文件:" + resultFileName + "在本目录生成...");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void getProjectAllFlowsAndAllJobs(String projectName) {
        //Map<String,Set> flowJobsMap = new HashMap<>();
        try {
            //get all the flows of this project
            String flowsProjectJson = fetchFlowsProject(projectName);
            JSONObject jsonProjectFlowParam = new JSONObject(flowsProjectJson);
            JSONArray flowsArray = jsonProjectFlowParam.getJSONArray("flows");
            //put all the flowId into a List
            List<String> flowIdList = new ArrayList();
            if (flowsArray != null && flowsArray.length() > 0) {
                for (int i = 0; i < flowsArray.length(); i++) {
                    JSONObject flowNode = (JSONObject) flowsArray.get(i);
                    flowIdList.add(flowNode.getString("flowId"));
                }
            }
            System.out.println("项目:" + projectName + "中含有的flowid有如下:");
            System.out.println("    " + Joiner.on(',').join(flowIdList));
            Set<String> jobSet = new HashSet();
            if (flowIdList != null && flowIdList.size() > 0) {
                for (int i = 0; i < flowIdList.size(); i++) {
                    jobSet.clear();
                    String flowId = flowIdList.get(i);
                    //have the flowId and then get all the jobs of this flowId
                    String jobsFlowJson = fetchJobsOfFlow(projectName, flowId);
                    JSONObject jsonJobFlowParam = new JSONObject(jobsFlowJson);
                    JSONArray jobsArray = jsonJobFlowParam.getJSONArray("nodes");
                    if (flowsArray != null && jobsArray.length() > 0) {
                        for (int k = 0; k < jobsArray.length(); k++) {
                            JSONObject jobNode = (JSONObject) jobsArray.get(k);
                            String jobId = jobNode.getString("id");
                            jobSet.add(jobId);
                        }
                    }
                    //flowJobsMap.put(flowId,jobSet);
                    System.out.println("Project:" + projectName + ",FlowId:" + flowId);
                    System.out.println("    Jobs:" + Joiner.on(",").join(jobSet));
                    Thread.sleep(5000);
                }
            }
            //for (Map.Entry<String, Set> entry : flowJobsMap.entrySet()) {
            //    String flowId = entry.getKey();
            //    System.out.println("Current FlowId is " + flowId);
            //    Set<String> jobSet = entry.getValue();
            //    for (String job : jobSet) {
            //        System.out.println("    " + job);
            //    }
            //}
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public MainClass() {
        super();
    }

    /**
     * Authenticate (logo in to get session id )
     * url: https://10.240.4.8:8443/index/?action=login
     */
    public static String getSessionId(String username, String password) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("index/");
            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("action", "login"));
            params.add(new BasicNameValuePair("username", username));
            params.add(new BasicNameValuePair("password", password));
            String responseData = HttpUtil.sendPost(urlSb.toString(), Constant.HTTP_POST_CONTENT_TYPE_URLENCODE, "", params);
            JSONObject jsonParam = new JSONObject(responseData);
            if (jsonParam != null && "success".equals(jsonParam.getString("status"))) {
                sessionId = jsonParam.getString("session.id");
            } else {
                log.error("登录Azakban出现错误....请检查....");
                return "";
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return sessionId;
    }


    /**
     * Create a Project
     */
    public static String createProject(String projectName, String description) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("manager/");
            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("action", "create"));
            params.add(new BasicNameValuePair("session.id", sessionId));
            params.add(new BasicNameValuePair("name", projectName));
            params.add(new BasicNameValuePair("description", description));
            String responseData = HttpUtil.sendPost(urlSb.toString(), Constant.HTTP_POST_CONTENT_TYPE_URLENCODE, "", params);
            //JSONObject jsonParam = new JSONObject(responseData);
            System.out.println(responseData);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return "";
    }

    /**
     * Delete a Project
     */
    public static String deleteProject(String projectName) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("manager?delete=true");
            urlSb.append("&session.id=" + sessionId);
            urlSb.append("&project=" + projectName);
            //the get request return html code not json also use postman and the api doc not metion the result
            HttpUtil.sendGet(urlSb.toString(), new HashMap());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Upload a Project
     */
    public static String uploadProject(String projectName, String zipFilePath) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("manager");
            File uploadFile = new File(zipFilePath);
            HttpEntity entity = MultipartEntityBuilder
                    .create()
                    .addTextBody("ajax", "upload")
                    .addTextBody("session.id", sessionId)
                    .addTextBody("project", projectName)
                    .addBinaryBody("file", uploadFile, ContentType.create("application/zip"), uploadFile.getName())
                    .build();
            HttpPost post = new HttpPost(urlSb.toString());
            post.setEntity(entity);
            HttpResponse response = HttpUtil.getHttpClient().execute(post);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Fetch Flows of a Project
     */
    public static String fetchFlowsProject(String projectName) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("manager?ajax=fetchprojectflows");
            urlSb.append("&session.id=" + sessionId);
            urlSb.append("&project=" + projectName);
            String responseData = HttpUtil.sendGet(urlSb.toString(), new HashMap());
            //System.out.println(responseData);
            //JSONObject jsonParam = new JSONObject(responseData);
            return responseData;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Fetch Jobs of a FlowSet
     * 在Azkaban原生接口的基础上(返回json字符串) 封装成Set集合.
     */
    public static Set<String> fetchJobSetOfFlow(String projectName, String flowName) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("manager?ajax=fetchflowgraph");
            urlSb.append("&session.id=" + sessionId);
            urlSb.append("&project=" + projectName);
            urlSb.append("&flow=" + flowName);
            String jobsFlowJson = HttpUtil.sendGet(urlSb.toString(), new HashMap());

            JSONObject jsonJobFlowParam = new JSONObject(jobsFlowJson);
            JSONArray jobsArray = jsonJobFlowParam.getJSONArray("nodes");
            Set<String> jobSet = new HashSet();
            if (jobsArray.length() > 0) {
                for (int k = 0; k < jobsArray.length(); k++) {
                    JSONObject jobNode = (JSONObject) jobsArray.get(k);
                    String jobId = jobNode.getString("id");
                    jobSet.add(jobId);
                }
            }
            //System.out.println(responseData);
            return jobSet;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Fetch Jobs of a Flow
     * 返回的是Azkaban原生接口对应的字符串
     */
    public static String fetchJobsOfFlow(String projectName, String flowName) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("manager?ajax=fetchflowgraph");
            urlSb.append("&session.id=" + sessionId);
            urlSb.append("&project=" + projectName);
            urlSb.append("&flow=" + flowName);
            String jobsFlowJson = HttpUtil.sendGet(urlSb.toString(), new HashMap());

            //System.out.println(responseData);
            return jobsFlowJson;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Fetch Executions of a Flow
     *
     * @return
     */
    public static String fecthExecutionsFlow(String projectName, String flowName, String startNum, String lengthNum) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("manager?ajax=fetchFlowExecutions");
            urlSb.append("&session.id=" + sessionId);
            urlSb.append("&project=" + projectName);
            urlSb.append("&flow=" + flowName);
            urlSb.append("&start=" + startNum);
            urlSb.append("&length=" + lengthNum);
            String responseData = HttpUtil.sendGet(urlSb.toString(), new HashMap());
            System.out.println(responseData);
            //JSONObject jsonParam = new JSONObject(responseData);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Fetch Running Executions of a Flow(获取一个工作流中正在运行的作业)
     */
    public static String fetchRunningExecutions(String projectName, String flowName) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("executor?ajax=getRunning");
            urlSb.append("&session.id=" + sessionId);
            urlSb.append("&project=" + projectName);
            urlSb.append("&flow=" + flowName);
            String responseData = HttpUtil.sendGet(urlSb.toString(), new HashMap());
            System.out.println(responseData);
            //JSONObject jsonParam = new JSONObject(responseData);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Retry Failed Jobs
     */
    public static String retryFailedJobs(String execid) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("executor?ajax=retryFailedJobs");
            urlSb.append("&session.id=" + sessionId);
            urlSb.append("&execid=" + execid);
            String responseData = HttpUtil.sendGet(urlSb.toString(), new HashMap());
            System.out.println(responseData);
            //JSONObject jsonParam = new JSONObject(responseData);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Flexible scheduling using Cron(使用Cron灵活调度)
     */
    public static String flexibleSchedulingCron(String projectName, String flow, String cronExpression) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("schedule");
            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("ajax", "scheduleCronFlow"));
            params.add(new BasicNameValuePair("action", "create"));
            params.add(new BasicNameValuePair("session.id", sessionId));
            params.add(new BasicNameValuePair("projectName", projectName));
            params.add(new BasicNameValuePair("flow", flow));
            params.add(new BasicNameValuePair("cronExpression", cronExpression));
            String responseData = HttpUtil.sendPost(urlSb.toString(), Constant.HTTP_POST_CONTENT_TYPE_URLENCODE, "", params);
            System.out.println(responseData);
            //JSONObject jsonParam = new JSONObject(responseData);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return "";
    }

    /**
     * Fetch a Schedule(获取调度)
     */
    public static String fetchSchedule(String projectId, String flowId) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("schedule?ajax=fetchSchedule");
            urlSb.append("&projectId=" + projectId);
            urlSb.append("&session.id=" + sessionId);
            urlSb.append("&flowId=" + flowId);
            String responseData = HttpUtil.sendGet(urlSb.toString(), new HashMap());
            System.out.println(responseData);
            //JSONObject jsonParam = new JSONObject(responseData);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Unschedule a Flow(取消一个工作流的调度计划)
     */
    public static String unscheduleFlow(String scheduleId) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("schedule");
            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("scheduleId", scheduleId));
            params.add(new BasicNameValuePair("action", "removeSched"));
            params.add(new BasicNameValuePair("session.id", sessionId));
            String responseData = HttpUtil.sendPost(urlSb.toString(), Constant.HTTP_POST_CONTENT_TYPE_URLENCODE, "", params);
            System.out.println(responseData);
            //JSONObject jsonParam = new JSONObject(responseData);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Set a SLA
     * curl -k -d "ajax=setSla&scheduleId=1&slaEmails=a@example.com;b@example.com&settings[0]=aaa,SUCCESS,5:00,true,false&settings[1]=bbb,SUCCESS,10:00,
     * false,true" -b "azkaban.browser.session.id=XXXXXXXXXXXXXX" "http://localhost:8081/schedule"
     */
    public static String setSLA() {
        return "";
    }

    /**
     * Fetch a SLA(获取SLA)
     */
    public static String fetchSLA(String scheduleId) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("schedule?ajax=slaInfo");
            urlSb.append("&session.id=" + sessionId);
            urlSb.append("&scheduleId=" + scheduleId);
            String responseData = HttpUtil.sendGet(urlSb.toString(), new HashMap());
            System.out.println(responseData);
            //JSONObject jsonParam = new JSONObject(responseData);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Pause a Flow Execution(暂停工作流的执行)
     * curl -k --data "session.id=34ba08fd-5cfa-4b65-94c4-9117aee48dda&ajax=pauseFlow&execid=303" https://localhost:8443/executor
     */
    public static String pauseFlowExecution(String execid) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("executor?ajax=pauseFlow");
            urlSb.append("&session.id=" + sessionId);
            urlSb.append("&execid=" + execid);
            String responseData = HttpUtil.sendGet(urlSb.toString(), new HashMap());
            System.out.println(responseData);
            //JSONObject jsonParam = new JSONObject(responseData);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Resume a Flow Execution(恢复一个工作流的执行)
     * curl -k --data "session.id=34ba08fd-5cfa-4b65-94c4-9117aee48dda&ajax=resumeFlow&execid=303" https://localhost:8443/executor
     */
    public static String resumeFlowExecution(String execid) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("executor?ajax=resumeFlow");
            urlSb.append("&session.id=" + sessionId);
            urlSb.append("&execid=" + execid);
            String responseData = HttpUtil.sendGet(urlSb.toString(), new HashMap());
            System.out.println(responseData);
            //JSONObject jsonParam = new JSONObject(responseData);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }


    /**
     * Fetch a Flow Execution(获取一个工作流的执行)
     */
    public static String fetchFlowExecution(String execid) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("executor?ajax=fetchexecflow");
            urlSb.append("&session.id=" + sessionId);
            urlSb.append("&execid=" + execid);
            String responseData = HttpUtil.sendGet(urlSb.toString(), new HashMap());
            System.out.println(responseData);
            //JSONObject jsonParam = new JSONObject(responseData);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * init the flow job dependcies
     *
     * @param projectName
     * @param flowName
     * @return
     */
    public static void initJobDependcy(String projectName, String flowName) {
        try {
            //在多次循环的调用不同项目的这个任务的时候全局变量jobChildrenMap  和 jobParentMap 会有之前的值干扰...所以先进行一次清空操作.
            jobChildrenMap.clear();
            jobParentMap.clear();
            String fetchflowgraph = fetchJobsOfFlow(projectName, flowName);
            JSONObject jsonParam = new JSONObject(fetchflowgraph);
            //判断请求是否有问题...
            if (jsonParam.has("error")) {
                String error = jsonParam.getString("error");
                if (!Strings.isNullOrEmpty(error)){
                    log.error("请求异常:" + error);
                    System.exit(-1);
                }
            }
            JSONArray nodesArray = jsonParam.getJSONArray("nodes");
            if (nodesArray != null && nodesArray.length() > 0) {
                for (int i = 0; i < nodesArray.length(); i++) {
                    //iterator the nodsArray init the parentSet for each job
                    HashSet<String> parentSet = null;
                    HashSet<String> childrenSet = null;
                    JSONObject node = (JSONObject) nodesArray.get(i);
                    String jobName = node.getString("id");
                    if (node.has("in")) {
                        parentSet = new HashSet<>();
                        String jobIn = node.getString("in");
                        String[] parentArray = jobIn.substring(1, jobIn.length() - 1).split(",");
                        for (int j = 0; j < parentArray.length; j++) {
                            String currentInJob = parentArray[j].substring(1, parentArray[j].length() - 1);
                            //init current job parent set
                            parentSet.add(currentInJob);
                            //init current in job children set
                            childrenSet = jobChildrenMap.get(currentInJob);
                            //currentInJob's children has already set
                            if (childrenSet != null) {
                                childrenSet.add(jobName);
                            } else {//currentInJob's children first set
                                childrenSet = new HashSet();
                                childrenSet.add(jobName);
                            }
                            jobChildrenMap.put(currentInJob, childrenSet);
                        }
                    } else {//the job do not have parent
                        parentSet = new HashSet<>();
                    }
                    jobParentMap.put(jobName, parentSet);
                }
            }
            //for the job which do not have children ,the above way jobChildrenMap do not have these elements
            Set<String> parentElements = jobParentMap.keySet();
            Set<String> childrenElements = jobChildrenMap.keySet();
            Set<String> resultSet = new HashSet<>();
            resultSet.addAll(parentElements);
            resultSet.removeAll(childrenElements);
            for (String result : resultSet) {
                jobChildrenMap.put(result, new HashSet());
            }
            System.out.println("");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * get job's parents 指定ProjectName 和 指定FlowName的 job的Parent信息
     */
    public static HashSet<String> getJobParents(String projectName, String flowName, String jobName) {
        try {
            //initJobDependcy(projectName, flowName);
            HashSet<String> parentSet = jobParentMap.get(jobName);
            System.out.println(jobName + ":所有的父亲如下:");
            for (String parent : parentSet) {
                System.out.println("    " + parent);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * get job's parents 重载方法
     */
    public static String getJobParents(String jobName) {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            HashSet<String> parentSet = jobParentMap.get(jobName);
            //如果在当前调度中找到了
            if(parentSet != null && parentSet.size() > 0){
                for (String parent : parentSet) {
                    stringBuilder.append("    " + parent + "\n");
                }
            }else{
                stringBuilder.append("    在当前指定的调度中没有数据!\n");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return stringBuilder.toString();
    }

    /**
     * get job's ancestors 指定的projectName 和 flowName的 job的祖先信息
     */
    public static HashSet getJobAncestors(String projectName, String flowName, String jobName) {
        try {
            //initJobDependcy(projectName,flowName);
            HashSet<String> jobAncestorSet = new HashSet<>();
            jobAncestorSet = initJobAncestorSet(jobName, jobAncestorSet);
            //对所有的该job的 祖先集合 遍历打印:
            System.out.println(jobName + ":的所有的祖先如下:");
            for (String jobAncestor : jobAncestorSet) {
                System.out.println("    " + jobAncestor);
            }
            return jobAncestorSet;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * get job's ancestors 重载方法
     */
    public static String getJobAncestors(String jobName) {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            HashSet<String> jobAncestorSet = new HashSet<>();
            jobAncestorSet = initJobAncestorSet(jobName, jobAncestorSet);
            //如果在当前调度中找到了
            if(jobAncestorSet != null && jobAncestorSet.size() > 0){
                //对所有的该job的 祖先集合 遍历打印:
                for (String jobAncestor : jobAncestorSet) {
                    stringBuilder.append("    " + jobAncestor + "\n");
                }
            }else{
                stringBuilder.append("    在当前指定的调度中没有数据!\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stringBuilder.toString();
    }

    private static HashSet<String> initJobAncestorSet(String jobName, HashSet<String> jobAncestorSet) {
        if (jobParentMap.get(jobName) != null) {
            if (jobParentMap.get(jobName).size() > 0) {
                HashSet<String> jobParentsSet = jobParentMap.get(jobName);
                jobAncestorSet.addAll(jobParentsSet);
                for (String job : jobParentsSet) {
                    initJobAncestorSet(job, jobAncestorSet);
                }
            } else {//the job do not have parents...it's the first node
                return jobAncestorSet;
            }
        } else {
            System.out.println("当前的Project-Flow中没有你指定的jobName:" + jobName + "请你检查!!");
            System.exit(-1);
        }

        return jobAncestorSet;
    }

    /**
     * get job's children 指定ProjectName 和 FlowName的
     */
    public static Set getJobChildren(String projectName, String flowName, String jobName) {
        try {
            initJobDependcy(projectName, flowName);
            HashSet<String> childrenSet = jobChildrenMap.get(jobName);
            System.out.println(jobName + ":所有的孩子后代如下:");
            for (String children : childrenSet) {
                System.out.println("    " + children);
            }
            return jobChildrenMap.get(jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * get job's children
     */
    public static String getJobChildren(String jobName) {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            HashSet<String> childrenSet = jobChildrenMap.get(jobName);
            if(childrenSet != null && childrenSet.size() > 0){
                //对所有的该job的 祖先集合 遍历打印:
                for (String children : childrenSet) {
                    stringBuilder.append("    " + children + "\n");
                }
            }else{
                stringBuilder.append("    在当前指定的调度中没有数据!\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stringBuilder.toString();
    }

    /**
     * get job's descendents 指定ProjectName 和 FlowName的
     */
    public static Set getJobDescendents(String projectName, String flowName, String jobName) {
        try {
            //initJobDependcy(projectName,flowName);
            HashSet<String> jobDescendentSet = new HashSet<>();
            jobDescendentSet = initJobDescendentSet(jobName, jobDescendentSet);
            System.out.println(jobName + ":所有的子孙后代如下:");
            for (String jobDescendent : jobDescendentSet) {
                System.out.println("    " + jobDescendent);
            }
            return jobDescendentSet;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * get job's descendents 重载方法
     */
    public static String getJobDescendents(String jobName) {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            HashSet<String> jobDescendentSet = new HashSet<>();
            jobDescendentSet = initJobDescendentSet(jobName, jobDescendentSet);
            if(jobDescendentSet != null && jobDescendentSet.size() > 0){
                //对所有的该job的 祖先集合 遍历打印:
                for (String jobDescendent : jobDescendentSet) {
                    stringBuilder.append("    " + jobDescendent + "\n");
                }
            }else{
                stringBuilder.append("    在当前指定的调度中没有数据!\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stringBuilder.toString();
    }

    public static String getJobString(String projectName, String flowName, String jobName) {
        Set descendentSet = getJobDescendents(projectName, flowName, jobName);
        String str = AzkabanUtil.set2String((HashSet) descendentSet);
        System.out.println(str);
        return str;
    }

    private static HashSet<String> initJobDescendentSet(String jobName, HashSet<String> jobDescendentSet) {
        if (jobChildrenMap.get(jobName) != null) {
            if (jobChildrenMap.get(jobName).size() > 0) {
                HashSet<String> jobChildrenSet = jobChildrenMap.get(jobName);
                jobDescendentSet.addAll(jobChildrenSet);
                for (String job : jobChildrenSet) {
                    initJobDescendentSet(job, jobDescendentSet);
                }
            } else {//the job do not have parents...it's the first node
                return jobDescendentSet;
            }
        } else {
            System.out.println("当前的Project-Flow中没有你指定的jobName:" + jobName + "请你检查!!");
            System.exit(-1);
        }
        return jobDescendentSet;
    }

    /**
     * Fetch Execution Job Logs(获取执行作业日志)
     * curl -k --data "session.id=90e&ajax=fetchExecJobLogs&execid=297&jobId=test-foobar&offset=0&length=100" https://localhost:8443/executor
     */
    public static String fetchExecutionJobLogs(String execid, String jobId, String offset, String length) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("executor?ajax=fetchExecJobLogs");
            urlSb.append("&session.id=" + sessionId);
            urlSb.append("&execid=" + execid);
            urlSb.append("&jobId=" + jobId);
            urlSb.append("&offset=" + offset);
            urlSb.append("&length=" + length);
            String responseData = HttpUtil.sendGet(urlSb.toString(), new HashMap());
            System.out.println(responseData);
            //JSONObject jsonParam = new JSONObject(responseData);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Fetch Flow Execution Updates(获取工作流执行更新)
     * curl -k --data "execid=301&lastUpdateTime=-1&session.id=66" https://localhost:8443/executor?ajax=fetchexecflowupdate
     */
    public static String fetchFlowExecutionUpdates(String execid, String lastUpdateTime) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("executor?ajax=fetchexecflowupdate");
            urlSb.append("&session.id=" + sessionId);
            urlSb.append("&execid=" + execid);
            urlSb.append("&lastUpdateTime=" + lastUpdateTime);
            String responseData = HttpUtil.sendGet(urlSb.toString(), new HashMap());
            System.out.println(responseData);
            //JSONObject jsonParam = new JSONObject(responseData);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Fetch Logs of a Project (获取一个项目的日志)
     *
     * @return
     */
    public static String fetchLogsProject(String projectName) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("manager?ajax=fetchProjectLogs");
            urlSb.append("&project=" + projectName);
            urlSb.append("&session.id=" + sessionId);
            String responseData = HttpUtil.sendGet(urlSb.toString(), new HashMap());
            System.out.println(responseData);
            //JSONObject jsonParam = new JSONObject(responseData);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * 获取指定executeid对应所有的Job执行情况
     */
    public static String fetchFlowInfo(String execid) {
        try {
            StringBuilder urlSb = new StringBuilder(Constant.URL_PART1).append("executor?ajax=flowInfo");
            urlSb.append("&execid=" + execid);
            urlSb.append("&session.id=" + sessionId);
            String responseData = HttpUtil.sendGet(urlSb.toString(), new HashMap());
            System.out.println(responseData);
            return responseData;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * 得到指定的
     *
     * @param execid
     * @return
     */
    public static List<String> fetchSuccessedFlowInfo(String execid) {
        try {
            String responseData = fetchFlowInfo(execid);
            JSONObject jsonParam = new JSONObject(responseData);
            JSONObject nodeStatusNode = jsonParam.getJSONObject("nodeStatus");
            Iterator keysIterator = nodeStatusNode.keys();
            List successedJobList = new ArrayList();
            while (keysIterator.hasNext()) {
                String key = (String) keysIterator.next();
                String value = nodeStatusNode.getString(key);
                if (Constant.SUCCESSED.equals(value)) {
                    successedJobList.add(key);
                }
            }
            return successedJobList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
