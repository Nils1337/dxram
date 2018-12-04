package de.hhu.bsinfo.dxram.boot;

import de.hhu.bsinfo.dxraft.client.ClientConfig;
import de.hhu.bsinfo.dxraft.client.RaftClient;
import de.hhu.bsinfo.dxraft.client.wrapper.DistributedAtomicInteger;
import de.hhu.bsinfo.dxraft.client.wrapper.DistributedDeque;
import de.hhu.bsinfo.dxraft.data.BooleanData;
import de.hhu.bsinfo.dxraft.data.ByteData;
import de.hhu.bsinfo.dxraft.data.RaftData;
import de.hhu.bsinfo.dxraft.data.ShortData;
import de.hhu.bsinfo.dxraft.server.RaftServer;
import de.hhu.bsinfo.dxraft.server.ServerConfig;
import de.hhu.bsinfo.dxraft.state.RaftEntry;
import de.hhu.bsinfo.dxram.boot.raft.Bitmap;
import de.hhu.bsinfo.dxram.boot.raft.UpdateBitmapOperation;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.BloomFilter;
import de.hhu.bsinfo.dxutils.CRC16;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DXRaftHandler implements ConsensusHandler {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXRaftHandler.class);

    private static final String FREE_ID_LIST_PATH = "free";
    private static final String COUNTER_PATH = "counter";
    private static final String BOOTSTRAP_PATH = "bootstrap";
    private static final String ID_BITMAP_PATH = "id-bitmap";

    private NodeDetails m_nodeDetails;
    private DXRaftHandlerConfig m_config;
    private RaftServer m_raftServer;
    private RaftClient m_raftClient;

    private DistributedDeque m_freeIdDeque;
    private DistributedAtomicInteger m_atomicInteger;
    private int m_counterValue = -1;

    public DXRaftHandler(DXRaftHandlerConfig p_config) {
        m_config = p_config;
    }


    @Override
    public NodeDetails getBootstrapDetails() {
        RaftEntry entry = m_raftClient.read(BOOTSTRAP_PATH, false);
        if (entry != null) {
            return (NodeDetails) entry.getData();
        }
        return null;
    }

    @Override
    public void freeNodeId(short p_id) {
        m_freeIdDeque.pushBack(new ShortData(p_id));
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
            m_raftServer = new RaftServer(config);
            if (!m_raftServer.bootstrapNewCluster()) {
                return false;
            }
        } else if (p_details.getRole() == NodeRole.SUPERPEER) {
            ServerConfig config = m_config.getRaftServerConfig();
            m_raftServer = new RaftServer(config);
            if (!m_raftServer.joinExistingCluster()) {
                return false;
            }
        }

        ClientConfig config = m_config.getRaftClientConfig();
        m_raftClient = new RaftClient(config);
        if (!m_raftClient.init()) {
            return false;
        }

        m_nodeDetails = p_details;
        m_atomicInteger = new DistributedAtomicInteger(m_raftClient, COUNTER_PATH);
        m_atomicInteger.init(1);
        m_freeIdDeque = new DistributedDeque(m_raftClient, FREE_ID_LIST_PATH);
        m_freeIdDeque.init();

        if (m_config.isBootstrapPeer()) {
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
        m_counterValue = 0;
        short id = calculateNodeId();
        m_nodeDetails.setId(id);

        // create new bitmap for ids, update with created id and write to raft
        Bitmap bitmap = new Bitmap(65536);
        bitmap.set(id, true);

        if (!m_raftClient.write(ID_BITMAP_PATH, bitmap, true)) {
            LOGGER.error("Creating id bitmap entry failed");
            return false;
        }

        // write node details of this node to the bootstrap path in raft
        if (!m_raftClient.write(BOOTSTRAP_PATH, m_nodeDetails, true)) {
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
            } catch (InterruptedException e) {
                // Ignored
            }

            bootstrapDetails = getBootstrapDetails();
        }

        LOGGER.info("Bootstrap node is ready");

        // Assign a globally unique id
        assignNodeId();

        return true;
    }

    /**
     * Assigns a globally unique id to this node.
     */
    private void assignNodeId() {

        // try to use free id
        RaftData idData = m_freeIdDeque.popFront();
        if (idData instanceof ShortData) {
            short id = ((ShortData) idData).getValue();
            m_nodeDetails.setId(id);
            LOGGER.info("Assigned previously freed id {} to this node", id);
            return;
        }

        // update bloom filter with id bitmap from raft
        Bitmap bitmap = (Bitmap) m_raftClient.read(ID_BITMAP_PATH, false).getData();
        BloomFilter bloomFilter = new BloomFilter((int) Math.pow(2, 20), 65536);

        if (bitmap == null) {
            throw new IllegalStateException("Failed reading bitmap from raft");
        }

        bitmap.forEach(id -> bloomFilter.add(id.shortValue()));

        // try to get unused id by incrementing distributed counter, hashing it,
        // checking for conflicts with the bloom filter and updating the id bitmap in raft
        // until it is successful
        while (true) {
            m_counterValue = m_atomicInteger.getAndIncrement();

            if (m_counterValue == -1) {
                throw new IllegalStateException("Incrementing atomic counter failed");
            }

            short id = calculateNodeId();
            if (bloomFilter.contains(id)) {
                // id is already in the bloom filter -> try again
                continue;
            }

            BooleanData result = (BooleanData) m_raftClient.applyAtomicOperation(ID_BITMAP_PATH,
                    new UpdateBitmapOperation(id));

            if (!result.isTrue()) {
                // someone already reserved this id -> try again
                continue;
            }

            m_nodeDetails.setId(id);
            LOGGER.info("Assigned calculated id {} based on the counter value {} to this node", id, m_counterValue);
            break;
        }

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
