/**
 *  Copyright (c) 2018 by the President and Fellows of Harvard College
 */
package edu.harvard.hul.ois.drs2.services.zookeeper;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 
 * @author dan179
 */
public class ZooKeeperService implements Watcher, Runnable {
	
	private static final Logger LOG = Logger.getLogger(ZooKeeperService.class);

	public static final String DRS_SERVICES_ROOT_NODE_PATH = "/drs_services";
	public static final String MASTER_NODE_PATH = DRS_SERVICES_ROOT_NODE_PATH + "/master";

	private boolean isMaster = false;
	private ZooKeeper zooKeeper;
	
	public ZooKeeperService(final String url) throws IOException {
		
		zooKeeper = new ZooKeeper(url, 3000, this);
		
		// attempt to create root node
		initRootNode();

		// try to create master node and claim
		isMaster = isMasterNode();
		LOG.info("**** isMaster: " + isMaster);
	}
	
	/**
	 * Attempt to obtain "ownership" of this node as "master".
	 * 
	 * @return <code>true</code> if this process is "master"; <code>false</code> otherwise.
	 */
	public boolean isMaster() {
		
		isMaster = attemptOwnership();
		return isMaster;
	}

	@Override
	public void process(WatchedEvent event) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("Event received: " + event);
		}
		
		final EventType eventType = event.getType();
		if( !EventType.None.equals(eventType)) {
			this.isMaster = attemptOwnership();
			LOG.info("**** isMaster: " + isMaster);
		}
	}
	
	/*
	 * if obtains master status return 'true'; otherwise return 'false'
	 */
	private boolean attemptOwnership() {
		initRootNode(); // just in case attempt to create root node
		return isMasterNode();
	}
	
	/*
	 * Attempts to create root node. Stateless method.
	 */
	private void initRootNode() {
		
		try {
			createNode(DRS_SERVICES_ROOT_NODE_PATH, false, false);

		} catch (KeeperException e) {

			Code exceptionCode = e.code();
			LOG.info("Exception Code: " + exceptionCode);
			
			// if node already exists then nothing to do
			if(Code.NODEEXISTS.equals(e.code())) {
				LOG.info("Node already exists: [" + DRS_SERVICES_ROOT_NODE_PATH + "]");
			} else {
				// this is an exceptional condition
				LOG.error("Problem creating node: [" + DRS_SERVICES_ROOT_NODE_PATH + "]", e);
				throw new IllegalStateException(e);
			}
		} catch (InterruptedException e) {
			LOG.error("Problem creating node: [" + DRS_SERVICES_ROOT_NODE_PATH + "]", e);
			throw new IllegalStateException(e);
		}
	}
	
	/*
	 * Attempts to create master node. If created then this process is "master".
	 * Stateless method.
	 */
	private boolean isMasterNode() {
		
		boolean ownsNode = false;
		try {
			createNode(MASTER_NODE_PATH, true, true);
			// node was created so we have ownership
			ownsNode = true;
			
		} catch (KeeperException e) {
			
			Code exceptionCode = e.code();
			LOG.info("Exception Code: " + exceptionCode);
			
			// if node already exists then some other instance is master
			if(Code.NODEEXISTS.equals(e.code())) {
				ownsNode = false; 
				LOG.info("Node already exists: [" + MASTER_NODE_PATH + "]");
				// always maintain watch on this node
				// need this if node not created
				watchNode(MASTER_NODE_PATH);
			} else {
				// this is an exceptional condition
				LOG.error("Problem creating node: [" + MASTER_NODE_PATH + "]", e);
				throw new IllegalStateException(e);
			}
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}

		return ownsNode;
	}

	/*
	 * Create a node.
	 * Throws KeeperException if node already exists (not fatal) or some other problem.
	 */
	private void createNode(final String node, final boolean watch, final boolean ephimeral)
			throws KeeperException, InterruptedException {
		
		String createdNodePath = zooKeeper.create(node, new byte[0], Ids.OPEN_ACL_UNSAFE, (ephimeral ?  CreateMode.EPHEMERAL : CreateMode.PERSISTENT));
		if (createdNodePath == null) {
			throw new IllegalStateException("Cannot have null node for path: [" + node + "]");
		}
		LOG.info("created node: [" + createdNodePath + "] -- ephimeral: " + ephimeral + ", watched: " + watch);

		if (watch) {
			zooKeeper.exists(createdNodePath, watch);
		}
	}
	
	/*
	 * Put a watch on the given node.
	 */
	private void watchNode(final String node) {
		
		try {
			Stat stat = zooKeeper.exists(node, true);
			if (stat != null) {
				LOG.info("watch put on node: [ " + node + "]");
			} else {
				LOG.warn("Cannot put on watch, node does not exist: " + node + "]");
			}
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException(e);
		}
	}

	/*
	 * For integration testing only
	 */
	@Override
	public void run() {
		
		if(LOG.isInfoEnabled()) {
			LOG.info("Process has started!");
		}
	}
}
