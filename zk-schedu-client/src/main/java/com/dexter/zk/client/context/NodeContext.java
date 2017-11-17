package com.dexter.zk.client.context;

import java.util.List;

public final class NodeContext {

	private static boolean leader;
	private static String taskPath;
	private static String leaderPath;
	private static String nodeId;
	private static String localHostAddress;
	//单节点处理任务数
	private static int singleNodeTaskNum;
	//最小节点数(满足该条件才开始调度工作)
	private static int minNodeNum;
	private static List<String> taskList;
	//是否允许任务重叠
	private static boolean isRepeat;

	public static String getLocalHostAddress() {
		return localHostAddress;
	}

	public static void setLocalHostAddress(String localHostAddress) {
		NodeContext.localHostAddress = localHostAddress;
	}

	public static boolean isLeader() {
		return leader;
	}

	public static void setLeader(boolean leader) {
		NodeContext.leader = leader;
	}

	public static boolean isRepeat() {
		return isRepeat;
	}

	public static void setRepeat(boolean isRepeat) {
		NodeContext.isRepeat = isRepeat;
	}

	public static List<String> getTaskList() {
		return taskList;
	}

	public static void setTaskList(List<String> taskArr) {
		taskList = taskArr;
	}

	public static int getSingleNodeTaskNum() {
		return singleNodeTaskNum;
	}

	public static void setSingleNodeTaskNum(int singleNodeTaskNum) {
		NodeContext.singleNodeTaskNum = singleNodeTaskNum;
	}

	public static int getMinNodeNum() {
		return minNodeNum;
	}

	public static void setMinNodeNum(int minNodeNum) {
		NodeContext.minNodeNum = minNodeNum;
	}

	public static String getTaskPath() {
		return taskPath;
	}

	public static String getLeaderPath() {
		return leaderPath;
	}

	public static String getNodeId() {
		return nodeId;
	}

	public static void setTaskPath(String taskPath) {
		NodeContext.taskPath = taskPath;
	}

	public static void setLeaderPath(String leaderPath) {
		NodeContext.leaderPath = leaderPath;
	}

	public static void setNodeId(String nodeId) {
		NodeContext.nodeId = nodeId;
	}

	private NodeContext(){
		
	}
}
