package com.dexter.zk.client.leader.event;

import com.dexter.zk.client.exceptions.ZookeepException;

public interface TaskNodeChangeListener {

	public abstract void nodeChange(String jsonData) throws ZookeepException;
}
