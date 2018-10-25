/**
 *  Copyright (c) 2018 by the President and Fellows of Harvard College
 */
package edu.harvard.hul.ois.drs2.services.zookeeper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * 
 * @author dan179
 */
public class MasterNode implements Runnable {
	
	private static final Logger LOG = Logger.getLogger(MasterNode.class);
	
//	public static final String DRS_SERVICES_ROOT_NODE = "/drs_services";
//	public static final String LEADER_NODE = "/leader";
//	private static final String PROCESS_NODE_PREFIX = "/p_";
//	private static final String MASTER = "isMaster";
	
	private ZooKeeperService zooKeeperService = null;
	
	private String leaderNodePath;
	private String watchedNodePath;
	
	public MasterNode(final String zkURL) throws IOException {
//		zooKeeperService = new ZooKeeperService(zkURL, new ProcessNodeWatcher());
	}
	
//	private void attemptForLeaderPosition() {
//		
//		final List<String> childNodePaths = zooKeeperService.getChildren(DRS_SERVICES_ROOT_NODE, false);
//		
//		Collections.sort(childNodePaths);
//		
//		int index = childNodePaths.indexOf(leaderNodePath.substring(leaderNodePath.lastIndexOf('/') + 1));
//		if(index == 0) {
//			if(LOG.isInfoEnabled()) {
//				LOG.info("I am the new leader!");
//			}
//		} else {
//			final String watchedNodeShortPath = childNodePaths.get(index - 1);
//			
//			watchedNodePath = DRS_SERVICES_ROOT_NODE + "/" + watchedNodeShortPath;
//			
//			if(LOG.isInfoEnabled()) {
//				LOG.info("Setting watch on node with path: " + watchedNodePath);
//			}
//			zooKeeperService.watchNode(watchedNodePath, true);
//		}
//	}

	@Override
	public void run() {
		
		if(LOG.isInfoEnabled()) {
			LOG.info("Process has started!");
		}
		
//		String rootNodePath = null;
//		try {
//			rootNodePath = zooKeeperService.createNode(DRS_SERVICES_ROOT_NODE, false, false);
//			if(rootNodePath == null) {
//				throw new IllegalStateException("Unable to create/access leader election root node with path: " + DRS_SERVICES_ROOT_NODE);
//			}
//		} catch (KeeperException | InterruptedException e) {
//			LOG.error("Uh oh 1", e);
//		}
//		
//		try {
//			leaderNodePath = zooKeeperService.createNode(rootNodePath + LEADER_NODE, false, true);
//		} catch (KeeperException | InterruptedException e) {
//			LOG.error("Uh oh 2", e);
//		}
//		if(leaderNodePath == null) {
//			throw new IllegalStateException("Unable to create/access process node with path: " + rootNodePath);
//		}
//		
//		if(LOG.isDebugEnabled()) {
//			LOG.debug("Process node created with path: " + leaderNodePath);
//		}

//		attemptForLeaderPosition();
	}
	
	public class ProcessNodeWatcher implements Watcher{

		@Override
		public void process(WatchedEvent event) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Event received: " + event);
			}
			
			final EventType eventType = event.getType();
			if(EventType.NodeDeleted.equals(eventType)) {
				if(event.getPath().equalsIgnoreCase(watchedNodePath)) {
//					attemptForLeaderPosition();
				}
			} else if (!EventType.None.equals(event.getType()) && watchedNodePath != null) {
//			} else if (!EventType.None.equals(event.getType()) && watchedNodePath != null && MasterNode.this.zooKeeperService.doesNodeExist(watchedNodePath, true)) {
				// on startup there is a NodeDataChanged event before there is a watchedNodePath established
				// Also, when a node is deleted AND has data, the watch node sends BOTH NodeDeleted then NodeDataChanged events
				if (EventType.NodeDataChanged.equals(event.getType()) && watchedNodePath != null) {
//					byte[] data = LeaderNode.this.zooKeeperService.getData(watchedNodePath);
//					ByteArrayOutputStream os = new ByteArrayOutputStream();
//					if (data != null) {
//						String d = null;
//						for (byte b : data) {
//							os.write(b);
//						}
//						d = os.toString();
//						LOG.debug("Node data: [" + d + "]");
//					}
				}
			}
			
		}
		
	}

}
