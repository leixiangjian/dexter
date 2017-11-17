package com.dexter.zk.client.leader;

import org.apache.curator.framework.CuratorFramework;

public interface LeaderChange {

	public abstract void leaderInit(CuratorFramework client);
	
	public abstract void leaderDestroy(CuratorFramework client);
}
