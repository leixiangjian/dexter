package com.dexter.zk.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dexter.zk.client.common.Bootstrap;
import com.dexter.zk.client.common.TaskDataConstants;
import com.dexter.zk.client.common.ZkUtils;
import com.dexter.zk.client.context.NodeContext;
import com.dexter.zk.client.exceptions.ZookeepException;
import com.dexter.zk.client.leader.LeaderChange;
import com.dexter.zk.client.leader.LeaderManager;
import com.dexter.zk.client.leader.event.TaskNodeChangeListener;

public abstract class ZkBootstrap implements Bootstrap {

	private static final Logger LOG = LoggerFactory.getLogger(ZkBootstrap.class);
	private CuratorFramework client;
	// 重试间隔时间
	private static volatile int baseSleepTime = Integer.getInteger("zk.base.sleep.time", 1000);
	// 重试连接次数
	private static volatile int maxRetries = Integer.getInteger("zk.max.retry.times", 5);
	// 连接超时时间
	private static volatile int connectionTimeout = Integer.getInteger("zk.connection.timeout", 5000);
	// 会话超时时间
	private static volatile int sessionTimeout = Integer.getInteger("zk.session.timeout", 10000);
	private static String taskPath = System.getProperty("zk.node.task.path", "/lock/task");
	private static String leaderPath = System.getProperty("zk.node.leader.path", "/lock/led");
	private static boolean isRepeat = Boolean.getBoolean("zk.node.task.repeat");
	private static String tempDataDir = System.getProperty("zk.node.leader.temp.data.path","/lock/temp");
	private static int hostMax = Integer.getInteger("zk.node.host.max", 0);
	// 单节点处理任务数
	private static int singleNodeTaskNum = Integer.getInteger("zk.single.node.tasknum", Integer.MAX_VALUE);
	// 最小节点数(大于等于该值才进行调度)
	private static int minNodeNum = Integer.getInteger("zk.min.node.num", 1);
	// 发布的任务
	private List<String> taskList;
	private static String nameSpace;
	private static String zkAddress;
	private String nodeId;
	private String taskData;
	private String hostIndex;
	private int taskIndex;
	protected static Properties properties;
	private LeaderChange leaderChange;
	
	public ZkBootstrap(LeaderChange leaderChange){
		this.leaderChange = leaderChange;
	}
	
	public ZkBootstrap(){
		
	}

	public void register() {
		notEmpty(nameSpace, "nameSpace is empty.");
		notEmpty(zkAddress, "zkAddress is empty.");
		if (null == taskList || taskList.size() == 0) {
			throw new ZookeepException("taskList is empty.");
		}
		initContext();
		registerTask();
	}

	private static String getDefaultNodeId(String ip) {
			return String.format("%s@%s", getPid(), ip);
	}
	
	private static String getHostAddress(){
		try {
			String ip = InetAddress.getLocalHost().getHostAddress();
			return ip;
		} catch (UnknownHostException e) {
			LOG.error(e.getMessage(), e);
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	protected static String getPid(){
		String name = ManagementFactory.getRuntimeMXBean().getName();
		return name.split("@")[0];
	}

	private void initContext() {
		NodeContext.setSingleNodeTaskNum(singleNodeTaskNum);
		NodeContext.setMinNodeNum(minNodeNum);
		NodeContext.setLeaderPath(leaderPath);
		NodeContext.setTaskPath(taskPath);
		NodeContext.setTaskList(taskList);
		NodeContext.setRepeat(isRepeat);
		NodeContext.setLocalHostAddress(getHostAddress());
		if (null == nodeId) {
			nodeId = getDefaultNodeId(NodeContext.getLocalHostAddress());
			NodeContext.setNodeId(nodeId);
		} else {
			NodeContext.setNodeId(nodeId);
		}
	}

	private void registerTask() {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTime, maxRetries);
		client = CuratorFrameworkFactory.builder().connectString(zkAddress).sessionTimeoutMs(sessionTimeout).connectionTimeoutMs(connectionTimeout).retryPolicy(retryPolicy)
				.namespace(nameSpace).build();
		client.start();
		Map<String, String> defaultData = new HashMap<String, String>();
		defaultData.put(TaskDataConstants.TASK_NODE_DATA_NODEID, NodeContext.getNodeId());
		defaultData.put(TaskDataConstants.TASK_NODE_DATA_NODE_HOST, NodeContext.getLocalHostAddress());
		ZkUtils.registerTask(client, taskPath, leaderPath, JSON.toJSONString(defaultData), new TaskNodeChangeListener() {
			public void nodeChange(String jsonData) throws ZookeepException {
				JSONObject dataMap = JSON.parseObject(jsonData);
				// 如果是自己,则调用启动方法
				if (dataMap.containsKey(TaskDataConstants.TASK_NODE_DATA_NODEID)&&nodeId.equals(dataMap.getString(TaskDataConstants.TASK_NODE_DATA_NODEID))) {
					if (LOG.isInfoEnabled()) {
						LOG.info("开始启动节点:{},通知数据:{}", nodeId, jsonData);
					}
					NodeContext.setLeader(dataMap.getString(TaskDataConstants.TASK_NODE_DATA_LEADERNODE).equals(nodeId));
					taskData = dataMap.getString(TaskDataConstants.TASK_NODE_DATA_TASK_DATA);
					hostIndex = dataMap.getString(TaskDataConstants.TASK_NODE_DATA_NODE_HOST_INDEX);
					taskIndex = dataMap.getInteger(TaskDataConstants.TASK_NODE_DATA_TASK_INDEX);
					start();
				}
			}
		}, new LeaderManager(leaderChange,tempDataDir,hostMax));
	}

	public void setBaseSleepTime(int baseSleepTime) {
		ZkBootstrap.baseSleepTime = baseSleepTime;
	}

	public void setMaxRetries(int maxRetries) {
		ZkBootstrap.maxRetries = maxRetries;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		ZkBootstrap.connectionTimeout = connectionTimeout;
	}

	public void setSessionTimeout(int sessionTimeout) {
		ZkBootstrap.sessionTimeout = sessionTimeout;
	}

	public void setTaskPath(String taskPath) {
		ZkBootstrap.taskPath = taskPath;
	}

	public void setLeaderPath(String leaderPath) {
		ZkBootstrap.leaderPath = leaderPath;
	}

	public void setNameSpace(String nameSpace) {
		ZkBootstrap.nameSpace = nameSpace;
	}

	public void setZkAddress(String zkAddress) {
		ZkBootstrap.zkAddress = zkAddress;
	}

	public void setSingleNodeTaskNum(int singleNodeTaskNum) {
		ZkBootstrap.singleNodeTaskNum = singleNodeTaskNum;
	}

	public void setTaskList(List<String> taskList) {
		this.taskList = taskList;
	}
	
	public void addTask(String task){
		if(null==taskList){
			taskList = new ArrayList<String>();
		}
		taskList.add(task);
	}

	public void setMinNodeNum(int minNodeNum) {
		ZkBootstrap.minNodeNum = minNodeNum;
	}

	private void notEmpty(String value, String msg) {
		if (null == value || "".equals(value)) {
			throw new ZookeepException(msg);
		}
	}

	public String getTaskData() {
		return taskData;
	}
	
	public Long getLongValue(String key){
		return getLongValue(key, 0);
	}
	
	public Long getLongValue(String key,long defaultValue){
		return Long.parseLong(getValue(key, defaultValue+""));
	}
	
	public Integer getIntValue(String key){
		return getIntValue(key, 0);
	}
	
	public Integer getIntValue(String key,int defaultValue){
		return Integer.parseInt(getValue(key, defaultValue+""));
	}
	
	public String getValue(String key){
		return getValue(key, null);
	}
	
	public static void setHostMax(int hostMax) {
		ZkBootstrap.hostMax = hostMax;
	}

	public static String getTempDataDir() {
		return tempDataDir;
	}

	public static void setTempDataDir(String tempDataDir) {
		ZkBootstrap.tempDataDir = tempDataDir;
	}

	public String getValue(String key,String defaultValue){
		return null != properties ? properties.getProperty(key, defaultValue) : null;
	}
	
	public String getHostIndex() {
		return hostIndex;
	}

	private static void initProperties(){
		baseSleepTime = Integer.parseInt(properties.getProperty("zk.base.sleep.time", "1000"));
		maxRetries = Integer.parseInt(properties.getProperty("zk.max.retry.times", "5"));
		connectionTimeout = Integer.parseInt(properties.getProperty("zk.connection.timeout", "5000"));
		sessionTimeout = Integer.parseInt(properties.getProperty("zk.session.timeout", "10000"));
		taskPath = properties.getProperty("zk.node.task.path", "/lock/task");
		leaderPath = properties.getProperty("zk.node.leader.path", "/lock/led");
		singleNodeTaskNum =  Integer.parseInt(properties.getProperty("zk.single.node.tasknum", Integer.MAX_VALUE+""));
		minNodeNum =  Integer.parseInt(properties.getProperty("zk.min.node.num", "1"));
		nameSpace = properties.getProperty("zk.namespace");
		zkAddress = properties.getProperty("zk.address.list");
		isRepeat = Boolean.parseBoolean(properties.getProperty("zk.node.task.repeat","false"));
		tempDataDir = properties.getProperty("zk.node.leader.temp.data.path", "/lock/temp");
		hostMax = Integer.parseInt(properties.getProperty("zk.node.host.max", "2"));
	}

	public static void loadProperties(String path) throws IOException{
		if(null==path){
			throw new RuntimeException("path is null");
		}
		properties = new Properties();
		InputStream is = null;
		try {
			if(path.startsWith(RES_CLASSPATH_FIX)){
				is = getClassPathResource(path);
			}else if(path.startsWith(RES_FILEPATH_FIX)){
				File file = new File(path.substring(path.indexOf(RES_FILEPATH_FIX)+RES_FILEPATH_FIX.length(),path.length()));
				is = new FileInputStream(file);
			}
			properties.load(is);
			initProperties();
		} finally{
			if(null!=is){
				is.close();
			}
		}
		
	}
	
	public int getTaskIndex() {
		return taskIndex;
	}

	private  static InputStream getClassPathResource(String path) throws IOException{
		if(path.startsWith(RES_CLASSPATH_FIX)){
			path = path.substring(path.indexOf(RES_CLASSPATH_FIX)+RES_CLASSPATH_FIX.length(),path.length());
		}
		InputStream is = ZkBootstrap.class.getResourceAsStream(path);
		if(null==is){
			is = ZkBootstrap.class.getClassLoader().getResourceAsStream(path);
		}
		if(null==is){
			throw new IOException(String.format("%s not found", path));
		}
		return is;
	}
	public void setLeaderChange(LeaderChange leaderChange) {
		this.leaderChange = leaderChange;
	}
	private static final String RES_CLASSPATH_FIX = "classpath:";
	private static final String RES_FILEPATH_FIX = "file:";
}
