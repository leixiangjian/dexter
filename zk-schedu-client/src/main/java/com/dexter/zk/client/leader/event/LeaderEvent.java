package com.dexter.zk.client.leader.event;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;

public interface LeaderEvent {

	public abstract void leaderInit(CuratorFramework client);
	
	public abstract void leaderDestroy(CuratorFramework client);
	
	public abstract void taskAdd(CuratorFramework client,ChildData childData);
	
	public abstract void taskRemove(CuratorFramework client,ChildData childData);
}