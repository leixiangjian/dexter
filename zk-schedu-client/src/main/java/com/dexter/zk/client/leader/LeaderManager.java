package com.dexter.zk.client.leader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dexter.zk.client.common.TaskDataConstants;
import com.dexter.zk.client.context.NodeContext;
import com.dexter.zk.client.exceptions.ZookeepException;
import com.dexter.zk.client.leader.event.LeaderEvent;
import com.dexter.zk.client.common.ZkUtils;

public class LeaderManager implements LeaderSelectorListener {

	private static final Logger LOG = LoggerFactory
			.getLogger(LeaderManager.class);

	private CountDownLatch latch = new CountDownLatch(1);

	private String appNodeId;

	private LeaderEvent leaderEvent;
	private LeaderChange leaderChange;

	private List<String> taskList;
	//已分配任务
	private List<String> allotList;
	private int singleNodeTaskNum;
	private String tempDataDir;
	//单机最多进程节点
	private int hostMax;
	private String tempDataPath;

	public LeaderManager() {

	}

	public LeaderManager(LeaderChange leaderChange, String tempDataDir,int hostMax) {
		this.leaderChange = leaderChange;
		this.tempDataDir = tempDataDir;
		this.hostMax = hostMax;
	}

	public void stateChanged(CuratorFramework client, ConnectionState newState) {
		if (LOG.isInfoEnabled()) {
			LOG.info("AppNodeId:{} stateChanged:{},leaderEvent:{},client:{}",
					appNodeId, newState, leaderEvent == null, client == null);
		}
		try {
			if (leaderEvent != null)
				leaderEvent.leaderDestroy(client);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		if (newState == ConnectionState.LOST) {
			if (LOG.isInfoEnabled()) {
				LOG.info("AppNodeId:{} destroying", appNodeId);
			}
			latch.countDown();
		}
	}

	public void takeLeadership(CuratorFramework client) throws Exception {
		appNodeId = NodeContext.getNodeId();
		if (LOG.isInfoEnabled()) {
			LOG.info("AppNodeId:{} takeLeader", appNodeId);
		}
		if (null == leaderEvent) {
			leaderEvent = new ProcessEvent();
		}
		taskList = NodeContext.getTaskList();
		allotList = new ArrayList<String>();
		singleNodeTaskNum = NodeContext.getSingleNodeTaskNum();
		leaderEvent.leaderInit(client);
		tempDataPath = ZkUtils.createEphemeralSequential(client,
				String.format("%s/%s", tempDataDir, appNodeId), "{}".getBytes());
		this.collectNodeData(client);
		ZkUtils.addPathChildrenListener(client, NodeContext.getTaskPath(),
				new PathChildrenCacheListener() {
					@SuppressWarnings("incomplete-switch")
					public void childEvent(CuratorFramework client,
							PathChildrenCacheEvent event) throws Exception {
						switch (event.getType()) {
						case CHILD_ADDED:
							leaderEvent.taskAdd(client, event.getData());
							break;
						case CHILD_REMOVED:
							leaderEvent.taskRemove(client, event.getData());
							break;
						}
					}

				});
		latch.await();
	}
	
	private void collectNodeData(CuratorFramework client) throws Exception {
		List<String> lstNode = client.getChildren().forPath(
				NodeContext.getTaskPath());
		if (null == lstNode || lstNode.size() == 0) {
			return;
		}
		Map<String, String> tMap = new HashMap<String, String>();
		for(String node : lstNode){
			String jsonData = getNodeJsonData(client, String.format("%s/%s", NodeContext.getTaskPath(),node));
			JSONObject json = JSONObject.parseObject(jsonData);
			if(json.containsKey(TaskDataConstants.TASK_NODE_DATA_TASK_DATA)){
				allotList.add(json.getString(TaskDataConstants.TASK_NODE_DATA_TASK_DATA));
			}
			if(json.containsKey(TaskDataConstants.TASK_NODE_DATA_NODE_HOST)&&json.containsKey(TaskDataConstants.TASK_NODE_DATA_NODE_HOST_INDEX)){
				String index = json.getString(TaskDataConstants.TASK_NODE_DATA_NODE_HOST_INDEX);
				if(tMap.containsKey(json.getString(TaskDataConstants.TASK_NODE_DATA_NODE_HOST))){
					String tValue = tMap.get(json.getString(TaskDataConstants.TASK_NODE_DATA_NODE_HOST));
					tMap.put(json.getString(TaskDataConstants.TASK_NODE_DATA_NODE_HOST), String.format("%s,%s", tValue,index));
				}else{
					tMap.put(json.getString(TaskDataConstants.TASK_NODE_DATA_NODE_HOST), index);
				}
			}
		}
		if(!tMap.isEmpty()){
			writeTempData(client, JSON.toJSONString(tMap));
		}
	}
	
	private String getTempNextIndex(CuratorFramework client,String host){
		if(hostMax <= 0){
			return "-1";
		}
		String jsonData = getNodeJsonData(client, tempDataPath);
		JSONObject json = JSONObject.parseObject(jsonData);
		if(!json.containsKey(host)){
			return "0";
		}
		String[] allIndex = json.getString(host).split(",");
		for(int i = 0 ; i < hostMax; i++){
			if(!exists(allIndex, i+"")){
				return i+"";
			}
		}
		return "-1";
	}
	
	private boolean exists(String[] allIndex,String code){
		for(String index : allIndex){
			if(code.equals(index)){
				return true;
			}
		}
		return false;
	}
	
	private String getNodeJsonData(CuratorFramework client, String nodePath) {
		try {
			return new String(client.getData().forPath(nodePath));
		} catch (Exception e) {
			throw new ZookeepException(e.getMessage(), e);
		}
	}
	
	private void writeTempData(CuratorFramework client, String jsonValue) {
		try {
			client.setData().forPath(tempDataPath, jsonValue.getBytes());
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw new ZookeepException(e.getMessage(), e);
		}
	}

	public void setLeaderChange(LeaderChange leaderChange) {
		this.leaderChange = leaderChange;
	}

	private class ProcessEvent implements LeaderEvent {

		public void leaderInit(CuratorFramework client) {
			try {
				if (null != leaderChange) {
					leaderChange.leaderInit(client);
				}
			} catch (Exception e) {
			}
		}

		public void leaderDestroy(CuratorFramework client) {
			try {
				if (null != leaderChange) {
					leaderChange.leaderDestroy(client);
				}
			} catch (Exception e) {
			}
			taskList = null;
		}

		public void taskAdd(CuratorFramework client, ChildData childData) {
			try {
				List<String> lstNode = client.getChildren().forPath(
						NodeContext.getTaskPath());
				if (lstNode.size() < NodeContext.getMinNodeNum()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("node join,current nodeSize:{},waiting....",
								lstNode.size());
					}
					return;
				}
				if (NodeContext.isRepeat()) {
					if (allotList.size() >= NodeContext.getMinNodeNum()) {
						return;
					}
					taskRepeat(lstNode, client);
				} else {
					if (allotList.size() == taskList.size()) {
						return;
					}
					for (int i = 1; i <= lstNode.size(); i++) {
						if (!NodeContext.isRepeat()&&allotList.size() == taskList.size()) {
							return;
						}
						int index = i;
						boolean result = false;
						do{
							lstNode = client.getChildren().forPath(
									NodeContext.getTaskPath());
							if (null==lstNode||lstNode.size() < NodeContext.getMinNodeNum()) {
								if (LOG.isDebugEnabled()) {
									LOG.debug("node join,current nodeSize:{},waiting....",
											lstNode.size());
								}
								return;
							}
							result = runIdelTask(index, client);
							index++;
						}while(!result);
					}
				}
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
				throw new ZookeepException(e.getMessage(), e);
			}
		}

		private void taskRepeat(List<String> lstNode, CuratorFramework client) {
			int tMinNodeNum = NodeContext.getMinNodeNum();
			for (int i = 1; i <= tMinNodeNum; i++) {
				runTask(0, taskList.size(),i, client);
				allotList.add(taskList.toString());
			}
		}

		public void taskRemove(CuratorFramework client, ChildData childData) {
			String jsonData = new String(childData.getData());
			if (LOG.isInfoEnabled()) {
				LOG.info("节点退出:{}", jsonData);
			}
			JSONObject jsonObj = JSON.parseObject(jsonData);
			removeTempData(jsonObj, client);
			if (jsonObj.containsKey(TaskDataConstants.TASK_NODE_DATA_TASK_DATA)) {
				String taskData = jsonObj
						.getString(TaskDataConstants.TASK_NODE_DATA_TASK_DATA);
				int taskIndex = jsonObj.getIntValue(TaskDataConstants.TASK_NODE_DATA_TASK_INDEX);
				boolean result = runIdelTask(client, taskData,taskIndex);
				if (!result) {
					LOG.warn("未发现空闲节点,任务数据:{},无法分配", taskData);
					if (NodeContext.isRepeat()) {
						if (allotList.size() > 0)
							allotList.remove(0);
					} else {
						allotList.removeAll(Arrays.asList(taskData.split(",")));
					}
				}
			}
		}
		
		private void removeTempData(JSONObject removeData,CuratorFramework client){
			if(!removeData.containsKey(TaskDataConstants.TASK_NODE_DATA_NODE_HOST_INDEX)){
				return;
			}
			String removeHostIndex =  removeData.getString(TaskDataConstants.TASK_NODE_DATA_NODE_HOST_INDEX);
			LOG.info("节点退出:{},移除 HOST_INDEX", removeHostIndex);
			String hostName = removeData.getString(TaskDataConstants.TASK_NODE_DATA_NODE_HOST);
			String jsonData = getNodeJsonData(client, tempDataPath);
			JSONObject jsonObj = JSONObject.parseObject(jsonData);
			if(jsonObj.containsKey(hostName)){
				String oldIndex = jsonObj.getString(hostName);
				String index = removeIndex(oldIndex.split(","), removeHostIndex);
				if(index == null){
					jsonObj.remove(hostName);
				}else{
					jsonObj.put(hostName, index);
				}
			}
			try {
				client.setData().forPath(tempDataPath, jsonObj.toString().getBytes());
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
				throw new ZookeepException(e.getMessage(), e);
				
			}
		}
		
		private String removeIndex(String[] oldIndex,String removeIndex){
			StringBuilder indexBuilder = new StringBuilder();
			for(String index : oldIndex){
				if(!index.equals(removeIndex)){
					indexBuilder.append(index).append(",");
				}
			}
			if(indexBuilder.length()>0){
				return indexBuilder.substring(0, indexBuilder.length()-1);
			}
			return null;
		}

		private boolean runIdelTask(int index, CuratorFramework client) {
			if (taskList.size() >= (index * singleNodeTaskNum)) {
				int start = index * singleNodeTaskNum - singleNodeTaskNum;
				int end = index * singleNodeTaskNum;
				return runTask(start, end,index, client);
			} else {
				// 将剩余任务指定给节点
				if (taskList.size() > allotList.size()) {
					int idelSize = taskList.size() - allotList.size();
					int start = taskList.size() - idelSize;
					int end = taskList.size();
					return runTask(start, end,index, client);
				}
			}
			return false;
		}

		private synchronized boolean runTask(int start, int end,int index, CuratorFramework client) {
			if(allotList.size()==taskList.size()){
				return true;
			}
			String taskData = getTask(start, end);
			if (taskData.length() > 0) {
				if(exists(taskData)){
					return false;
				}
				runIdelTask(client, taskData,index);
				if (!NodeContext.isRepeat())
					allotList.addAll(Arrays.asList(taskData.split(",")));
			}
			return true;
		}

		private boolean exists(String taskData){
			String[] taskArray = taskData.split(",");
			for(String task : taskArray){
				if(allotList.contains(task)){
					return true;
				}
			}
			return false;
		}
		
		private String getTask(int start, int end) {
			StringBuilder taskData = new StringBuilder();
			for (int j = start; j < end; j++) {
				if (taskData.length() > 0) {
					taskData.append(",");
				}
				taskData.append(taskList.get(j));
			}
			return taskData.toString();
		}

		/**
		 * 获取空闲任务节点
		 * 
		 * @return String
		 * @throws Exception
		 */
		private Node getIdleTaskNode(CuratorFramework client) {
			try {
				List<String> lstNode = getTaskNode(client);
				Collections.sort(lstNode);
				for (String taskNode : lstNode) {
					String nodePath = String.format("%s/%s",
							NodeContext.getTaskPath(), taskNode);
					String jsonData = getNodeJsonData(client, nodePath);
					JSONObject jsonObj = JSON.parseObject(jsonData);
					if (!jsonObj
							.containsKey(TaskDataConstants.TASK_NODE_DATA_TASK_DATA)) {
						return new Node(nodePath, jsonData);
					}
				}
				return null;
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
				throw new ZookeepException(e.getMessage(), e);
			}
		}

		public class Node {
			private String path;
			private String jsonData;
			private JSONObject jsonObj;

			Node(String path, String jsonData) {
				this.path = path;
				this.jsonData = jsonData;
				this.jsonObj = JSON.parseObject(jsonData);
			}

			String getPath() {
				return path;
			}

			String getValue(String key) {
				return jsonObj.getString(key);
			}

			String getJsonData() {
				return jsonData;
			}
		}

		/**
		 * 运行空闲任务
		 * 
		 * @param client
		 * @param groupId
		 * @throws Exception
		 *             void
		 */
		private boolean runIdelTask(CuratorFramework client, String taskData,int index) {
			Node node = this.getIdleTaskNode(client);
			if (LOG.isDebugEnabled()) {
				LOG.debug("获取空闲节点,path:{}", node.getPath());
			}
			if (null!=node&&null != node.getPath()) {
				 String regHost = node.getValue(TaskDataConstants.TASK_NODE_DATA_NODE_HOST);
				 String nextIndex = getTempNextIndex(client, regHost);
				this.workTaskDataWrite(client, node, taskData,nextIndex,index+"");
				return true;
			}
			return false;
		}

		/**
		 * 获取所有子节点
		 * 
		 * @param client
		 * @return
		 * @throws Exception
		 *             List<String>
		 */
		public List<String> getTaskNode(CuratorFramework client)
				throws Exception {
			return client.getChildren().forPath(NodeContext.getTaskPath());
		}


		/**
		 * 给任务分配工作内容
		 * 
		 * @param client
		 * @param nodePath
		 * @throws Exception
		 *             void
		 */
		private void workTaskDataWrite(CuratorFramework client, Node node,
				String taskData,String hostIndex,String taskIndex) {
			try {
				// byte[] nodeData = client.getData().forPath(nodePath);
				// if (null == nodeData) {
				// LOG.error("{} node data is null", nodePath);
				// throw new
				// ZookeepException(String.format("{} node data is null",
				// nodePath));
				// }
				// String jsonData = new String(node.);
				// if (LOG.isDebugEnabled()) {
				// LOG.debug("processing nodePath:{},nodeData:{},taskData:{}",
				// nodePath, jsonData, taskData);
				// }
				JSONObject jsonObj = JSON.parseObject(node.getJsonData());
				jsonObj.put(TaskDataConstants.TASK_NODE_DATA_TASK_INDEX,
						taskIndex);
				jsonObj.put(TaskDataConstants.TASK_NODE_DATA_TASK_DATA,
						taskData);
				jsonObj.put(TaskDataConstants.TASK_NODE_DATA_LEADERNODE,
						appNodeId);
				jsonObj.put(TaskDataConstants.TASK_NODE_DATA_NODE_HOST_INDEX, hostIndex);
				
				String jsonData = getNodeJsonData(client, tempDataPath);
				JSONObject nodeHostJson = JSON.parseObject(jsonData);
				String hostName = jsonObj.getString(TaskDataConstants.TASK_NODE_DATA_NODE_HOST);
				if(nodeHostJson.containsKey(hostName)){
					String oldIndex = nodeHostJson.getString(hostName);
					nodeHostJson.put(hostName, String.format("%s,%s", oldIndex,hostIndex));
				}else{
					nodeHostJson.put(hostName, hostIndex);
				}
				client.setData().forPath(tempDataPath,nodeHostJson.toJSONString().getBytes());
				client.setData().forPath(node.getPath(),
						jsonObj.toJSONString().getBytes());
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
				throw new ZookeepException(e.getMessage(), e);
			}
		}

	}

}
