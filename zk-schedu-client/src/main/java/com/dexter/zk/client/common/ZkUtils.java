package com.dexter.zk.client.common;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexter.zk.client.exceptions.ZookeepException;
import com.dexter.zk.client.leader.LeaderManager;
import com.dexter.zk.client.leader.event.TaskNodeChangeListener;

public final class ZkUtils {
	
	private static final Logger LOG = LoggerFactory.getLogger(ZkUtils.class);
	/**
	 * leader随机选举
	 * 
	 * @param client
	 * @param leaderPath
	 * @param selectorListener
	 *            void
	 */
	public static final LeaderSelector leaderShip(CuratorFramework client, final String leaderPath, LeaderSelectorListener selectorListener) {
		LeaderSelector leaderSelector = new LeaderSelector(client, leaderPath, selectorListener);
		leaderSelector.start();
		return leaderSelector;
	}
	
	
	/**
	 * 按文件序列创建文件节点
	 * 
	 * @param client
	 * @param path
	 * @param data
	 *            void
	 * @throws ZookeepException
	 */
	public static final String createEphemeralSequential(CuratorFramework client, String path, byte[] data) throws ZookeepException {
		try {
			if(null!=data){
				return client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, data);
			}
			return client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw new ZookeepException(e.getMessage(), e);
		}
	}

	/**
	 * 注册任务
	 * 
	 * @param client
	 * @param parentTaskPath
	 * @param taskData
	 * @return PathChildrenCache
	 * @throws ZookeepException
	 */
	public static final void registerTask(CuratorFramework client, String taskPath,final String leaderPath, String defaultData, final TaskNodeChangeListener changeListener,
			final LeaderManager leaderManager) throws ZookeepException {
		try {
			addPathChildrenListener(client, taskPath, new PathChildrenCacheListener() {
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					switch (event.getType()) {
					case CHILD_ADDED:
						ZkUtils.leaderShip(client, leaderPath, leaderManager);
						break;
					case CHILD_UPDATED:
						changeListener.nodeChange(event.getData() != null ? new String(event.getData().getData()) : null);
						break;
					}
				}
			});
			createEphemeralSequential(client, String.format("%s/%s", taskPath, "node"), null!=defaultData?defaultData.getBytes():null);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw new ZookeepException(e.getMessage(), e);
		}
	}

	public static final void addPathChildrenListener(CuratorFramework client, String taskPath, PathChildrenCacheListener changeListener) {
		PathChildrenCache task = new PathChildrenCache(client, taskPath, true);
		task.getListenable().addListener(changeListener);
		try {
			task.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw new ZookeepException(e.getMessage(), e);
		}
	}
	
	private ZkUtils(){
		
	}
}
