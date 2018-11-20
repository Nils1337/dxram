package de.hhu.bsinfo.dxram.boot;

import de.hhu.bsinfo.dxraft.client.ClientConfig;
import de.hhu.bsinfo.dxraft.client.RaftClient;
import de.hhu.bsinfo.dxraft.data.ByteData;
import de.hhu.bsinfo.dxraft.data.RaftData;
import de.hhu.bsinfo.dxraft.server.RaftServer;
import de.hhu.bsinfo.dxraft.server.ServerConfig;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.CRC16;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DXRaftNodeRegistry implements NodeRegistry {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXRaftNodeRegistry.class);

    private static final String NODES_PATH = "nodes";
    private static final String COUNTER_PATH = "counter";
    private static final String BOOTSTRAP_PATH = "bootstrap";

    private RaftClient m_client;
    private NodeDetails m_nodeDetails;
    private NodeRegistryListener m_listener;
    private DXRaftNodeRegistryConfig m_config;
    private RaftServer m_raftServer;
    private RaftClient m_raftClient;
    private int m_counterValue = -1;

    public DXRaftNodeRegistry(DXRaftNodeRegistryConfig p_config) {
        m_config = p_config;
    }

    @Override
    public void registerListener(NodeRegistryListener p_listener) {
        m_listener = p_listener;
    }

    @Override
    public NodeDetails getDetails() {
        return m_nodeDetails;
    }

    @Override
    public Collection<NodeDetails> getAll() {
        List<RaftData> nodes = m_client.readList(NODES_PATH);
        return nodes.stream()
                .map(data -> NodeDetails.fromByteArray(((ByteData)data).getData()))
                .collect(Collectors.toList());
    }

    @Override
    public NodeDetails getDetails(short p_nodeId) {
        List<RaftData> nodes = m_client.readList(NODES_PATH);
        if (nodes != null) {
            Optional<NodeDetails> nodeDetails = nodes.stream()
                    .map(data -> NodeDetails.fromByteArray(((ByteData) data).getData()))
                    .filter(details -> details.getId() == p_nodeId)
                    .findAny();
            if (nodeDetails.isPresent()) {
                return nodeDetails.get();
            }
        }
        return null;
    }

    @Override
    public NodeDetails getBootstrapDetails() {
        return NodeDetails.fromByteArray(((ByteData)m_raftClient.read(BOOTSTRAP_PATH)).getData());
    }

    @Override
    public void updateNodeDetails(NodeDetails p_details) {
        // TODO
    }

    @Override
    public void close() {
        if (m_raftServer != null) {
            m_raftServer.shutdown();
        }
        m_raftClient.shutdown();
    }

    @Override
    public boolean start(NodeDetails p_details) {
        if (m_config.isBootstrapPeer()) {
            ServerConfig config = m_config.getRaftServerConfig();
            config.setEnableBroadcasting(true);
            config.setServerMessagingService("dxnet");
            config.setClientMessagingService("dxnet");
            config.setUseStaticClusterAtBoot(false);
            m_raftServer = new RaftServer(config);
            if (!m_raftServer.bootstrapNewCluster()) {
                return false;
            }
        } else if (p_details.getRole() == NodeRole.SUPERPEER) {
            ServerConfig config = m_config.getRaftServerConfig();
            config.setEnableBroadcasting(true);
            config.setServerMessagingService("dxnet");
            config.setClientMessagingService("dxnet");
            config.setUseStaticClusterAtBoot(false);
            m_raftServer = new RaftServer(config);
            if (!m_raftServer.joinExistingCluster()) {
                return false;
            }
        }

        ClientConfig config = m_config.getRaftClientConfig();
        config.setMessagingService("dxnet");
        config.setUseBroadcast(true);
        m_raftClient = new RaftClient(config);
        if (!m_raftClient.init()) {
            return false;
        }

        m_nodeDetails = p_details;

        // Assign a globally unique counter value to this superpeer
        if (p_details.getRole() == NodeRole.SUPERPEER) {
            assignNodeId();
        }

        if (m_nodeDetails.getId() == 0) {
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

        return true;
    }

    /**
     * Initializes this node as the bootstrap node.
     *
     * @return true, if initialization succeeded; false else
     */
    private boolean initializeBootstrapNode() {
        LOGGER.info("Starting bootstrap process");

        if (!m_raftClient.write(BOOTSTRAP_PATH, new ByteData(m_nodeDetails.toByteArray()), true)) {
            LOGGER.error("Creating bootstrap entry failed");
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
        if (m_counterValue == -1) {
            assignNodeId();
        }

        return true;
    }

    /**
     * Assigns a globally unique id to this node.
     */
    private void assignNodeId() {
        m_counterValue = m_raftClient.getAndIncrement(COUNTER_PATH);

        if (m_counterValue == -1) {
            throw new IllegalStateException("Incrementing atomic counter failed");
        }

        m_nodeDetails.setId(calculateNodeId());

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
}
