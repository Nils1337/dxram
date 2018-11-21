/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.boot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import de.hhu.bsinfo.dxutils.CRC16;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import de.hhu.bsinfo.dxram.util.NodeCapabilities;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;

@SuppressWarnings("WeakerAccess")
public class ZookeeperHandler implements ServiceCacheListener, ConsensusHandler {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ZookeeperHandler.class);
    private static final RetryPolicy RETRY_POLICY = new ExponentialBackoffRetry(1000, 3);

    private static final String BASE_DIR = "/dxram";
    private static final String COUNTER_PATH = String.format("%s/counter", BASE_DIR);
    private static final String BOOTSTRAP_NODE_PATH = String.format("%s/boot", BASE_DIR);

    private static final String BASE_PATH = "/dxram";
    private static final String SERVICE_NAME = "nodes";
    private static final String THREAD_NAME = "discovery";
    private static final String DISCOVERY_PATH = String.format("%s/%s", BASE_PATH, SERVICE_NAME);

    private static final int INVALID_COUNTER_VALUE = -1;
    private static final int BOOTSTRAP_COUNTER_VALUE = 1;

    private CuratorFramework m_curatorClient;
    private final List<NodeRegistryListener> m_listener = new ArrayList<>();
    private final JsonInstanceSerializer<NodeDetails> serializer = new JsonInstanceSerializer<>(NodeDetails.class);
    private final AtomicBoolean m_isRunning = new AtomicBoolean(false);

    private ServiceDiscovery<NodeDetails> m_serviceDiscovery;
    private ServiceInstance<NodeDetails> m_instance;
    private ServiceCache<NodeDetails> m_cache;
    private ZookeeperHandlerConfig m_config;
    private DistributedAtomicInteger m_counter;
    private int m_counterValue = INVALID_COUNTER_VALUE;
    private NodeDetails m_details;

    private final ConcurrentHashMap<Short, NodeDetails> m_serviceMap = new ConcurrentHashMap<>();

    private final ExecutorService m_executor = Executors.newSingleThreadExecutor(
            runnable -> new Thread(runnable, THREAD_NAME));

    private enum ListenerEvent {
        PEER_JOINED, PEER_LEFT, SUPERPEER_JOINED, SUPERPEER_LEFT, NODE_UPDATED
    }

    ZookeeperHandler(ZookeeperHandlerConfig p_config) {
        m_config = p_config;
    }

    /**
     * Add a listener
     *
     * @param p_listener
     *         Listener
     */
    public void registerListener(final NodeRegistryListener p_listener) {
        m_listener.add(p_listener);
    }

    /**
     * Starts the node registry and service discovery.
     *
     * @param p_details
     *         This node's details.
     * @throws Exception
     *         On ZooKeeper errors.
     */
    public boolean start(final NodeDetails p_details) {
        if (m_isRunning.get()) {
            LOGGER.warn("Registry is already running");
            return false;
        }

        m_details = p_details;

        LOGGER.info("Initializing with address %s:%d and role %s", p_details.getAddress(), p_details.getPort(),
                p_details.getRole());

        String zooKeeperAddress = String.format("%s:%d", m_config.getConnection().getIP(),
                m_config.getConnection().getPort());

        LOGGER.info("Connecting to ZooKeeper at %s", zooKeeperAddress);

        // Connect to Zookeeper
        m_curatorClient = CuratorFrameworkFactory.newClient(zooKeeperAddress, RETRY_POLICY);
        m_curatorClient.start();

        m_counter = new DistributedAtomicInteger(m_curatorClient, COUNTER_PATH, RETRY_POLICY);

        // Assign a globally unique counter value to this superpeer
        if (p_details.getRole() == NodeRole.SUPERPEER) {
            assignNodeId();
        }

        if (isBootstrapNode()) {
            // Start bootstrap node initialization process if this is the first superpeer
            if (!initializeBootstrapNode()) {
                LOGGER.error("Initialization as bootstrap node failed");
                return false;
            }
        } else {
            // Start normal node initialization process if this is not the first superpeer
            if (!initializeNormalNode()) {
                LOGGER.error("Initialization as normal node failed");
                return false;
            }
        }

        return startServiceDiscovery();
    }

    private boolean startServiceDiscovery() {
        m_serviceMap.put(m_details.getId(), m_details);

        m_instance = m_details.toServiceInstance(SERVICE_NAME);

        m_serviceDiscovery = ServiceDiscoveryBuilder.builder(NodeDetails.class)
                .client(m_curatorClient)
                .basePath(BASE_PATH)
                .serializer(serializer)
                .thisInstance(m_instance)
                .build();

        m_cache = m_serviceDiscovery.serviceCacheBuilder()
                .name(SERVICE_NAME)
                .executorService(m_executor)
                .build();

        m_cache.addListener(this);

        // Start cache first so that it gets updated immediately
        try {
            m_cache.start();
            m_serviceDiscovery.start();
        } catch (Exception e) {
            LOGGER.error("Error initializing zookeeper", e);
            return false;
        }

        return true;
    }

    /**
     * Closes the node registry and stops service discovery.
     */
    @Override
    public void close() {
        CloseableUtils.closeQuietly(m_cache);
        CloseableUtils.closeQuietly(m_serviceDiscovery);
        m_isRunning.set(false);
        m_curatorClient.close();
    }

    /**
     * Initializes this node as the bootstrap node.
     *
     * @return true, if initialization succeeded; false else
     */
    private boolean initializeBootstrapNode() {
        LOGGER.info("Starting bootstrap process");

        try {
            // Insert own node information into ZooKeeper so other nodes can find the bootstrap node
            m_curatorClient.create().creatingParentsIfNeeded().forPath(BOOTSTRAP_NODE_PATH, m_details.toByteArray());
        } catch (Exception p_e) {
            LOGGER.error("Creating bootstrap entry failed", p_e);
            return false;
        }

        LOGGER.info("Finished bootstrap process");

        return true;
    }

    /**
     * Initializes this node as a normal node.
     *
     * @return true, if initialization succeeded; false else
     */
    private boolean initializeNormalNode() {
        LOGGER.info("Waiting on bootstrap node to finish initialization");

        // Wait until bootstrap node finishes initializing
        NodeDetails bootstrapDetails = getBootstrapDetails();

        while (bootstrapDetails == null) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException p_e) {
                // Ignored
            }

            bootstrapDetails = getBootstrapDetails();
        }

        LOGGER.info("Bootstrap node is ready");

        // Assign a globally unique counter value in case this is a peer, which hasn't assigned it yet
        if (m_counterValue == INVALID_COUNTER_VALUE) {
            assignNodeId();
        }

        return true;
    }

    /**
     * Assigns a globally unique id to this node.
     */
    private void assignNodeId() {
        AtomicValue<Integer> atomicValue;

        try {
            atomicValue = m_counter.increment();
        } catch (Exception p_e) {
            throw new RuntimeException(p_e);
        }

        if (!atomicValue.succeeded()) {
            throw new IllegalStateException("Incrementing atomic counter failed");
        }

        m_counterValue = atomicValue.postValue();
        m_details.setId(calculateNodeId());

        LOGGER.info("Assigned counter value %d to this node", m_counterValue);
    }

    /**
     * Calculates this node's id based on its unique counter value.
     *
     * @return This node's id.
     */
    private short calculateNodeId() {
        int seed = 1;
        short nodeId = 0;

        for (int i = 0; i < m_counterValue; i++) {
            nodeId = CRC16.continuousHash(seed, nodeId);
            seed++;
        }

        return nodeId;
    }

    /**
     * Updates a specific node's details within ZooKeeper.
     *
     * @param p_details
     *         The node's details.
     */
    public void updateNodeDetails(NodeDetails p_details) {
        ServiceInstance<NodeDetails> serviceInstance = p_details.toServiceInstance(SERVICE_NAME);

        if (serviceInstance.getId().equals(m_instance.getId())) {
            m_instance = serviceInstance;
        }

        try {
            m_serviceDiscovery.updateService(serviceInstance);
        } catch (Exception p_e) {
            LOGGER.warn("Updating registry entry failed", p_e);
        }
    }

    /**
     * Looks up the bootstrap node's details and returns them.
     *
     * @return The bootstrap node's details.
     */
    @Override
    public @Nullable NodeDetails getBootstrapDetails() {
        byte[] bootBytes;

        try {
            bootBytes = m_curatorClient.getData().forPath(BOOTSTRAP_NODE_PATH);
        } catch (Exception p_e) {
            return null;
        }

        return NodeDetails.fromByteArray(bootBytes);
    }

    @Override
    public void freeNodeId(short p_id) {
        // TODO
    }

    /**
     * Indicates if this node is responsible for the bootstrap process.
     *
     * @return True if this node is the bootstrap node; false else.
     */
    private boolean isBootstrapNode() {
        return m_counterValue == BOOTSTRAP_COUNTER_VALUE;
    }

    /**
     * Informs the registered listener about changes.
     *
     * @param p_event
     *         The event.
     * @param p_nodeDetails
     *         The node's details.
     */
    private void notifyListener(final ListenerEvent p_event, final NodeDetails p_nodeDetails) {
        //        runOnMainThread(() -> {
        for (NodeRegistryListener listener : m_listener) {
            switch (p_event) {
                case PEER_JOINED:
                    LOGGER.info("Node %s with capabilities %s joined the network",
                            p_nodeDetails, NodeCapabilities.toString(p_nodeDetails.getCapabilities()));

                    listener.onPeerJoined(p_nodeDetails);
                    break;

                case SUPERPEER_JOINED:
                    LOGGER.info("Node %s with capabilities %s joined the network",
                            p_nodeDetails, NodeCapabilities.toString(p_nodeDetails.getCapabilities()));

                    listener.onSuperpeerJoined(p_nodeDetails);
                    break;

                case PEER_LEFT:
                    LOGGER.info("Node %s with capabilities %s left the network",
                            p_nodeDetails, NodeCapabilities.toString(p_nodeDetails.getCapabilities()));

                    listener.onPeerLeft(p_nodeDetails);
                    break;

                case SUPERPEER_LEFT:
                    LOGGER.info("Node %s with capabilities %s left the network",
                            p_nodeDetails, NodeCapabilities.toString(p_nodeDetails.getCapabilities()));

                    listener.onSuperpeerLeft(p_nodeDetails);
                    break;

                case NODE_UPDATED:
                    LOGGER.info("Updated node %s with capabilities %s",
                            p_nodeDetails, NodeCapabilities.toString(p_nodeDetails.getCapabilities()));

                    listener.onNodeUpdated(p_nodeDetails);
                    break;
            }
        }
        //        });
    }

    /**
     * Returns the specified node's details.
     *
     * @param p_nodeId
     *         The node's id.
     * @return The specified node's details.
     */
    //@Override
    public @Nullable NodeDetails getDetails(final short p_nodeId) {
        NodeDetails details = m_serviceMap.get(p_nodeId);

        return details != null ? details : getRemoteDetails(p_nodeId);
    }

    /**
     * Retrieves the specified node's details from ZooKeeper.
     *
     * @param p_nodeId
     *         The node's id.
     * @return The specified node's details.
     */
    private NodeDetails getRemoteDetails(short p_nodeId) {
        byte[] bootBytes;

        try {
            bootBytes = m_curatorClient.getData().forPath(
                    String.format("%s/%s", DISCOVERY_PATH, NodeID.toHexStringShort(p_nodeId)));
        } catch (Exception p_e) {
            return null;
        }

        try {
            return serializer.deserialize(bootBytes).getPayload();
        } catch (Exception p_e) {
            LOGGER.warn("Couldn't deserialize remote node details");
            return null;
        }
    }

    /**
     * Returns this node's details.
     *
     * @return This node's details.
     */
    //@Override
    public @Nullable NodeDetails getDetails() {
        NodeDetails details = m_instance.getPayload();

        if (details == null) {
            return null;
        }

        return details;
    }

    /**
     * Returns all NodeDetails this node knows of.
     *
     * @return All known NodeDetails.
     */
    //@Override
    public Collection<NodeDetails> getAll() {
        return m_serviceMap.values();
    }

    /**
     * Called when the cache has changed (instances added/deleted, etc.)
     */
    @Override
    public void cacheChanged() {
        LOGGER.info("Service discovery cache changed");

        final Set<NodeDetails> remoteDetails = m_cache.getInstances().stream()
                .map(ServiceInstance::getPayload)
                .collect(Collectors.toSet());

        final Set<NodeDetails> localDetails = new HashSet<>(m_serviceMap.values());

        final Sets.SetView<NodeDetails> changedNodes = Sets.difference(remoteDetails, localDetails);
        for (NodeDetails details : changedNodes) {
            NodeDetails oldDetails = m_serviceMap.put(details.getId(), details);

            if (oldDetails != null) {
                notifyListener(ListenerEvent.NODE_UPDATED, details);
                continue;
            }

            if (details.getRole() == NodeRole.SUPERPEER) {
                notifyListener(ListenerEvent.SUPERPEER_JOINED, details);
            } else {
                notifyListener(ListenerEvent.PEER_JOINED, details);
            }
        }

        final Set<Short> remoteIds = remoteDetails.stream().map(NodeDetails::getId).collect(Collectors.toSet());
        final Set<Short> localIds = localDetails.stream().map(NodeDetails::getId).collect(Collectors.toSet());

        final Sets.SetView<Short> leftNodeIds = Sets.difference(localIds, remoteIds);
        NodeDetails leftNode;
        for (Short nodeId : leftNodeIds) {
            leftNode = m_serviceMap.get(nodeId);

            if (!leftNode.isOnline()) {
                continue;
            }

            m_serviceMap.put(nodeId, leftNode.withOnline(false));

            if (leftNode.getRole() == NodeRole.SUPERPEER) {
                notifyListener(ListenerEvent.SUPERPEER_LEFT, leftNode);
            } else {
                notifyListener(ListenerEvent.PEER_LEFT, leftNode);
            }
        }
    }

    /**
     * Called when there is a state change in the connection.
     *
     * @param p_client
     *         The client.
     * @param p_newState
     *         The new state.
     */
    @Override
    public void stateChanged(CuratorFramework p_client, ConnectionState p_newState) {
        LOGGER.info("Curator connection state changed to {}", p_newState.isConnected() ? "CONNECTED" : "DISCONNECTED");
    }

}
