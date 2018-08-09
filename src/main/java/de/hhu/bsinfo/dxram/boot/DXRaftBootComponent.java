package de.hhu.bsinfo.dxram.boot;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import de.hhu.bsinfo.dxraft.client.RaftClient;
import de.hhu.bsinfo.dxraft.context.RaftAddress;
import de.hhu.bsinfo.dxraft.context.RaftContext;
import de.hhu.bsinfo.dxraft.context.RaftID;
import de.hhu.bsinfo.dxraft.data.RaftData;
import de.hhu.bsinfo.dxraft.data.ShortData;
import de.hhu.bsinfo.dxraft.server.RaftServer;
import de.hhu.bsinfo.dxraft.server.RaftServerContext;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.boot.raft.DXRaftNetworkService;
import de.hhu.bsinfo.dxram.boot.raft.PeerData;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.events.NodeJoinEvent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.BloomFilter;
import de.hhu.bsinfo.dxutils.CRC16;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

public class DXRaftBootComponent extends AbstractBootComponent<DXRaftBootComponentConfig> implements
        EventListener<AbstractEvent> {

    private NetworkComponent m_network;

    private RaftServer m_raftServer;
    private RaftClient m_raftClient;

    // component dependencies
    private EventComponent m_event;
    private LookupComponent m_lookup;

    // private state
    private IPV4Unit m_ownAddress;

    private BloomFilter m_bloomFilter;

    private NodesConfiguration m_nodes;

    private volatile boolean m_isStarting;

    private boolean m_shutdown;

    /**
     * Constructor
     */
    public DXRaftBootComponent() {
        super(DXRAMComponentOrder.Init.BOOT, DXRAMComponentOrder.Shutdown.BOOT, DXRaftBootComponentConfig.class);
    }

    @Override
    public List<BackupPeer> getPeersFromNodeFile() {
        NodesConfiguration.NodeEntry[] allNodes = m_nodes.getNodes();

        NodesConfiguration.NodeEntry currentEntry;
        ArrayList<BackupPeer> ret = new ArrayList<>();
        for (int i = 0; i < allNodes.length; i++) {
            currentEntry = allNodes[i];
            if (currentEntry != null) {
                if (currentEntry.readFromFile() && currentEntry.getRole() == NodeRole.PEER) {
                    ret.add(new BackupPeer((short) (i & 0xFFFF), currentEntry.getRack(), currentEntry.getSwitch()));
                }
            }
        }

        return ret;
    }

    @Override
    public List<BackupPeer> getIDsOfAvailableBackupPeers() {
        NodesConfiguration.NodeEntry[] allNodes = m_nodes.getNodes();

        NodesConfiguration.NodeEntry currentEntry;
        ArrayList<BackupPeer> ret = new ArrayList<>();
        for (int i = 0; i < allNodes.length; i++) {
            currentEntry = allNodes[i];
            if (currentEntry != null) {
                if (currentEntry.getRole() == NodeRole.PEER && currentEntry.getStatus() &&
                        currentEntry.isAvailableForBackup()) {
                    ret.add(new BackupPeer((short) (i & 0xFFFF), currentEntry.getRack(), currentEntry.getSwitch()));
                }
            }
        }

        return ret;
    }

    @Override
    public ArrayList<NodesConfiguration.NodeEntry> getOnlineNodes() {
        return m_nodes.getOnlineNodes();
    }

    @Override
    public void putOnlineNodes(ArrayList<NodesConfiguration.NodeEntry> p_onlineNodes) {
        for (NodesConfiguration.NodeEntry entry : p_onlineNodes) {
            m_nodes.addNode(entry);
        }
    }

    @Override
    public List<Short> getIDsOfOnlineNodes() {
        NodesConfiguration.NodeEntry[] allNodes = m_nodes.getNodes();

        NodesConfiguration.NodeEntry currentEntry;
        ArrayList<Short> ret = new ArrayList<>();
        for (int i = 0; i < allNodes.length; i++) {
            currentEntry = allNodes[i];
            if (currentEntry != null) {
                if (currentEntry.getStatus()) {
                    ret.add((short) (i & 0xFFFF));
                }
            }
        }

        return ret;
    }

    @Override
    public List<Short> getIDsOfOnlinePeers() {
        NodesConfiguration.NodeEntry[] allNodes = m_nodes.getNodes();

        NodesConfiguration.NodeEntry currentEntry;
        ArrayList<Short> ret = new ArrayList<>();
        for (int i = 0; i < allNodes.length; i++) {
            currentEntry = allNodes[i];
            if (currentEntry != null) {
                if (currentEntry.getRole() == NodeRole.PEER && currentEntry.getStatus()) {
                    ret.add((short) (i & 0xFFFF));
                }
            }
        }

        return ret;
    }

    @Override
    public List<Short> getIDsOfOnlineSuperpeers() {
        NodesConfiguration.NodeEntry[] allNodes = m_nodes.getNodes();

        NodesConfiguration.NodeEntry currentEntry;
        ArrayList<Short> ret = new ArrayList<>();
        for (int i = 0; i < allNodes.length; i++) {
            currentEntry = allNodes[i];
            if (currentEntry != null) {
                if (currentEntry.getRole() == NodeRole.SUPERPEER && currentEntry.getStatus()) {
                    ret.add((short) (i & 0xFFFF));
                }
            }
        }

        return ret;
    }

    @Override
    public List<Short> getSupportingNodes(final int p_capabilities) {

        return Arrays.stream(m_nodes.getNodes())
                .filter(node -> NodeCapabilities.supportsAll(node.getCapabilities(), p_capabilities))
                .map(NodesConfiguration.NodeEntry::getNodeID)
                .collect(Collectors.toList());
    }

    @Override
    public short getNodeID() {
        return m_nodes.getOwnNodeID();
    }

    @Override
    public NodeRole getNodeRole() {
        return m_nodes.getOwnNodeEntry().getRole();
    }

    @Override
    public int getNodeCapabilities() {
        return m_nodes.getOwnNodeEntry().getCapabilities();
    }

    @Override
    public void updateNodeCapabilities(int p_capibilities) {
        m_nodes.getOwnNodeEntry().setCapabilities(p_capibilities);
    }

    @Override
    public short getRack() {
        return m_nodes.getOwnNodeEntry().getRack();
    }

    @Override
    public short getSwitch() {
        return m_nodes.getOwnNodeEntry().getSwitch();
    }

    @Override
    public int getNumberOfAvailableSuperpeers() {
        // if bootstrap is not available (wrong startup order of superpeers and peers)
        return getIDsOfOnlineSuperpeers().size();
    }

    @Override
    public short getNodeIDBootstrap() {
        // if bootstrap is not available (wrong startup order of superpeers and peers)
        RaftAddress leader = m_raftClient.getCurrentLeader();
        if (leader != null) {
            return leader.getId().getValue();
        }

        return NodeID.INVALID_ID;
    }

    @Override
    public boolean isNodeOnline(final short p_nodeID) {
        NodesConfiguration.NodeEntry entry = m_nodes.getNode(p_nodeID);
        if (entry == null) {

            LOGGER.warn("Could not find node %s", NodeID.toHexString(p_nodeID));

            return false;
        }

        return entry.getStatus();
    }

    @Override
    public NodeRole getNodeRole(final short p_nodeID) {
        NodesConfiguration.NodeEntry entry = m_nodes.getNode(p_nodeID);
        if (entry == null) {

            LOGGER.warn("Could not find node %s", NodeID.toHexString(p_nodeID));

            return null;
        }

        return entry.getRole();
    }

    @Override
    public int getNodeCapabilities(short p_nodeId) {
        NodesConfiguration.NodeEntry entry = m_nodes.getNode(p_nodeId);
        if (entry == null) {

            LOGGER.warn("Could not find node %s", NodeID.toHexString(p_nodeId));

            return 0;
        }

        return entry.getCapabilities();
    }

    @Override
    public InetSocketAddress getNodeAddress(final short p_nodeID) {
        NodesConfiguration.NodeEntry entry = m_nodes.getNode(p_nodeID);
        InetSocketAddress address;
        // return "proper" invalid address if entry does not exist
        if (entry == null) {

            //LOGGER.warn("Could not find ip and port for node id %s", NodeID.toHexString(p_nodeID));

            address = new InetSocketAddress("255.255.255.255", 0xFFFF);
        } else {
            address = entry.getAddress().getInetSocketAddress();
        }

        return address;
    }

    @Override
    public boolean nodeAvailable(final short p_nodeID) {
        return isNodeOnline(p_nodeID);
    }

    @Override
    public void singleNodeCleanup(final short p_nodeID, final NodeRole p_role) {

        if (p_role == NodeRole.SUPERPEER) {
            // Remove superpeer
            if (!m_nodes.getNode(p_nodeID).readFromFile()) {
                // Enable re-usage of NodeID if failed superpeer was not in nodes file
                m_raftClient.addToList("free", new ShortData(p_nodeID), true);
            }
            LOGGER.debug("Removed superpeer 0x%X from raft", p_nodeID);
        } else if (p_role == NodeRole.PEER) {
            // Remove peer
            if (!m_nodes.getNode(p_nodeID).readFromFile()) {
                // Enable re-usage of NodeID if failed peer was not in nodes file
                m_raftClient.addToList("free", new ShortData(p_nodeID), true);
            }
            LOGGER.debug("Removed peer 0x%X from raft", p_nodeID);
        }

        if (!m_nodes.getNode(p_nodeID).readFromFile()) {
            // Remove node from "new nodes"
            m_raftClient.removeFromList("new", new ShortData(p_nodeID), false);
        }

        m_nodes.getNode(p_nodeID).setStatus(false);
    }

    @Override
    public void eventTriggered(final AbstractEvent p_event) {
        if (p_event instanceof NodeFailureEvent) {
            m_nodes.getNode(((NodeFailureEvent) p_event).getNodeID()).setStatus(false);
        } else if (p_event instanceof NodeJoinEvent) {
            NodeJoinEvent event = (NodeJoinEvent) p_event;

            LOGGER.info(String.format("Node %s with capabilities %s joined", NodeID.toHexString(event.getNodeID()),
                    NodeCapabilities.toString(event.getCapabilities())));

            boolean readFromFile = m_nodes.getNode(event.getNodeID()) != null;
            m_nodes.addNode(new NodesConfiguration.NodeEntry(event.getAddress(),
                    event.getNodeID(), event.getRack(), event.getSwitch(),
                    event.getRole(), event.getCapabilities(), readFromFile,
                    event.isAvailableForBackup(), true));
        }
    }

    @Override
    public boolean finishInitComponent() {
        // Set own status to online
        m_nodes.getOwnNodeEntry().setStatus(true);
        m_isStarting = false;
        return true;
    }

    @Override
    protected boolean supportsSuperpeer() {
        return true;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_shutdown = true;

        if (m_lookup != null && m_lookup.isResponsibleForBootstrapCleanup()) {

            LOGGER.info("Shutting down raft server");
            m_raftServer.shutdown();

        } else {
            // LookupComponent has not been initialized or this node is not responsible for clean-up

            if (m_nodes.getOwnNodeEntry().getRole() == NodeRole.PEER) {
                // Remove own stuff from ZooKeeper for reboot
                singleNodeCleanup(m_nodes.getOwnNodeID(), NodeRole.PEER);
            }

            m_raftServer.shutdown();
        }

        return true;
    }

    // -----------------------------------------------------------------------------------

    /**
     * Assigns ids to every node in config
     *
     * @param p_nodes
     *         the nodes to parse
     * @param p_cmdLineNodeRole
     *         the role from command line
     * @return seed for continued hashing
     */
    private boolean assignIds(final ArrayList<NodesConfiguration.NodeEntry> p_nodes, final NodeRole p_cmdLineNodeRole) {
        short nodeID;
        int seed;

        LOGGER.trace("Entering parseNodes");

        // Parse node information
        seed = 1;

        // assign ids to all nodes in config file
        for (NodesConfiguration.NodeEntry entry : p_nodes) {
            nodeID = CRC16.continuousHash(seed);

            while (m_bloomFilter.contains(nodeID) || nodeID == NodeID.INVALID_ID) {
                nodeID = CRC16.continuousHash(++seed);
            }

            seed++;
            m_bloomFilter.add(nodeID);

            if (m_ownAddress.equals(entry.getAddress())) {
                if (entry.getRole() != p_cmdLineNodeRole) {

                    LOGGER.error("NodeRole in configuration differs from command line given NodeRole: %s != %s",
                            entry.getRole(), p_cmdLineNodeRole);

                    return false;
                }

                m_nodes.setOwnNodeID(nodeID);
                LOGGER.info("Own node assigned: %s", entry);
            }

            entry.setNodeID((short) (nodeID & 0x0000FFFF));
            m_nodes.addNode(entry);

            LOGGER.info("Node added: %s", entry);
        }

        // Apply changes
//        childs = m_raftClient.readList("new");
//        for (RaftData child : childs) {
//
//            PeerData data = (PeerData) child;
//            nodeID = data.getId();
//            m_bloomFilter.add(nodeID);
//
//            m_nodes.addNode(
//                    new NodesConfiguration.NodeEntry(new IPV4Unit(data.getIp(), data.getPort()), nodeID,
//                            data.getCmdLineRack(), data.getCmdLineSwitch(),
//                            NodeRole.toNodeRole(String.valueOf(data.getCmdLineRole())), false, true));
//
//            if (nodeID == m_nodes.getOwnNodeID()) {
//                // NodeID was already re-used
//                m_nodes.setOwnNodeID(NodeID.INVALID_ID);
//            }
//        }

        LOGGER.trace("Exiting parseNodes");
        return true;
    }

    /**
     * Assigns new id from raft to local node
     *
     * @param p_cmdLineNodeRole
     *        the role from command line
     * @param p_cmdLineRack
     *        the rack this node is in (irrelevant for nodes in nodes file)
     * @param p_cmdLineSwitch
     *        the switch this node is connected to (irrelevant for nodes in nodes file)
     */
    private void assignIdFromRaft(final NodeRole p_cmdLineNodeRole,
            final short p_cmdLineRack, final short p_cmdLineSwitch) {
        PeerData node;
        List<RaftData> childs;
        String[] splits;
        int seed;
        short nodeID = NodeID.INVALID_ID;

        // Add this node if it was not in start configuration
        LOGGER.warn("Node not in nodes.config (%s)", m_ownAddress);

        // get free id from raft
        boolean obtained = false;
        childs = m_raftClient.readList("free");
        while (!obtained && childs != null && !childs.isEmpty()) {
            nodeID = ((ShortData) childs.get(0)).getData();
            obtained = m_raftClient.removeFromList("free", new ShortData(nodeID), false);
            childs = m_raftClient.readList("free");
        }

        if (obtained) {
            node = new PeerData(nodeID, m_ownAddress.getIP(),
                    m_ownAddress.getPort(), p_cmdLineNodeRole.getAcronym(),
                    p_cmdLineRack, p_cmdLineSwitch);
            m_nodes.setOwnNodeID(nodeID);
            m_raftClient.addToList("new", node, true);
        } else {
            splits = m_ownAddress.getIP().split("\\.");
            seed = (Integer.parseInt(splits[1]) << 16) + (Integer.parseInt(splits[2]) << 8) + Integer.parseInt(
                    splits[3]);
            nodeID = CRC16.continuousHash(seed);
            while (m_bloomFilter.contains(nodeID) || nodeID == NodeID.INVALID_ID) {
                nodeID = CRC16.continuousHash(--seed);
            }
            m_bloomFilter.add(nodeID);
            // Set own NodeID
            m_nodes.setOwnNodeID(nodeID);
            node = new PeerData(nodeID, m_ownAddress.getIP(),
                    m_ownAddress.getPort(), p_cmdLineNodeRole.getAcronym(),
                    p_cmdLineRack, p_cmdLineSwitch);
            m_raftClient.addToList("new", node, true);
        }
    }


    @Override
    protected void resolveComponentDependencies(DXRAMComponentAccessor p_componentAccessor) {
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_event = p_componentAccessor.getComponent(EventComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
    }

    @Override
    protected boolean initComponent(DXRAMContext.Config p_config) {
        m_ownAddress = p_config.getEngineConfig().getAddress();
        NodeRole role = p_config.getEngineConfig().getRole();

        LOGGER.info("Initializing with address %s, role %s", m_ownAddress, role);

        m_event.registerListener(this, NodeFailureEvent.class);
        m_event.registerListener(this, NodeJoinEvent.class);

        m_isStarting = true;

        m_bloomFilter = new BloomFilter((int) getConfig().getBitfieldSize().getBytes(), 65536);
        m_nodes = new NodesConfiguration();

        if (!assignIds(getConfig().getNodesConfig(), role)) {
            LOGGER.error("Parsing nodes failed");
            return false;
        }

        initRaft(role);

        if (m_nodes.getOwnNodeID() == NodeID.INVALID_ID) {
            assignIdFromRaft(role, getConfig().getRack(), getConfig().getSwitch());
        } else {
            // Remove NodeID if this node failed before
            short nodeID = m_nodes.getOwnNodeID();
            m_raftClient.removeFromList("free", new ShortData(nodeID), false);
        }

        return true;
    }

    private void initRaft (NodeRole p_role) {
        NodesConfiguration.NodeEntry[] nodes = m_nodes.getNodes();
        List<RaftAddress> addresses = new ArrayList<>(nodes.length);

        for (NodesConfiguration.NodeEntry node: nodes) {
            addresses.add(new RaftAddress(new RaftID(node.getNodeID()), node.getAddress().getIP(),
                    node.getAddress().getPort()));
        }

        RaftAddress localAddress = new RaftAddress(new RaftID(m_nodes.getOwnNodeID()),
                m_nodes.getOwnNodeEntry().getAddress().getIP(),
                m_nodes.getOwnNodeEntry().getAddress().getPort());
        DXRaftNetworkService network = new DXRaftNetworkService(m_network);

        if (p_role == NodeRole.SUPERPEER) {
            RaftServerContext context = RaftServerContext.RaftServerContextBuilder
                    .aRaftServerContext()
                    .withRaftServers(addresses)
                    .withLocalAddress(localAddress)
                    .build();
            m_raftServer = RaftServer.RaftServerBuilder
                    .aRaftServer()
                    .withNetworkService(network)
                    .withContext(context)
                    .build();
            m_raftServer.bootstrapNewCluster();
        }

        RaftContext clientContext = new RaftContext(addresses, localAddress);
        m_raftClient = new RaftClient(clientContext, network);
    }
}
