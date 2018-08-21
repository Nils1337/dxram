package de.hhu.bsinfo.dxram.boot;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.gson.Gson;

import de.hhu.bsinfo.dxraft.client.RaftClient;
import de.hhu.bsinfo.dxraft.context.RaftAddress;
import de.hhu.bsinfo.dxraft.context.RaftContext;
import de.hhu.bsinfo.dxraft.data.RaftData;
import de.hhu.bsinfo.dxraft.data.ShortData;
import de.hhu.bsinfo.dxraft.data.StringData;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.events.NodeJoinEvent;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.BloomFilter;
import de.hhu.bsinfo.dxutils.CRC16;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

public class DXRaftBootComponent extends AbstractBootComponent<DXRaftBootComponentConfig> implements
        EventListener<AbstractEvent> {

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
        ShortData leader = (ShortData) m_raftClient.read("bootstrap");

        if (leader != null) {
            return leader.getData();
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

            //try to become superpeer if failed node was bootstrap
            m_raftClient.compareAndSet("bootstrap", new ShortData(m_nodes.getOwnNodeID()), new ShortData(p_nodeID));

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
            m_nodes.addNode(new NodesConfiguration.NodeEntry(event.getAddress(), event.getNodeID(), event.getRack(), event.getSwitch(),
                    event.getRole(), event.getCapabilities(), readFromFile,
                    event.isAvailableForBackup(), true, -1));
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

            //TODO clean up raft
//            LOGGER.info("Cleaning up raft");
//            m_raftServer.shutdown();

        } else {
            // LookupComponent has not been initialized or this node is not responsible for clean-up
            if (m_nodes.getOwnNodeEntry().getRole() == NodeRole.PEER) {
                // Remove own stuff from ZooKeeper for reboot
                singleNodeCleanup(m_nodes.getOwnNodeID(), NodeRole.PEER);
            }
        }

        return true;
    }

    // -----------------------------------------------------------------------------------

    /**
     * Parses the configured nodes
     *
     * @param p_nodes
     *         the nodes to parse
     * @param p_cmdLineNodeRole
     *         the role from command line
     * @param p_cmdLineRack
     *         the rack this node is in (irrelevant for nodes in nodes file)
     * @param p_cmdLineSwitch
     *         the switch this node is connected to (irrelevant for nodes in nodes file)
     * @return the parsed nodes
     */
    private boolean parseNodes(final ArrayList<NodesConfiguration.NodeEntry> p_nodes, final NodeRole p_cmdLineNodeRole,
            final short p_cmdLineRack, final short p_cmdLineSwitch) {
        short nodeID = NodeID.INVALID_ID;
        int seed;
        List<RaftData> childs;
        String[] splits;
        m_bloomFilter = new BloomFilter((int) getConfig().getBitfieldSize().getBytes(), 65536);

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

        // try to become bootstrap superpeer
        if (p_cmdLineNodeRole == NodeRole.SUPERPEER && m_nodes.getOwnNodeID() != NodeID.INVALID_ID) {
            boolean success = m_raftClient.compareAndSet("bootstrap", new ShortData(m_nodes.getOwnNodeID()), null);
            if (success) {
                LOGGER.info("Node became bootstrap superpeer");
            }
        }

        // Remove NodeID if this node failed before
        if (m_nodes.getOwnNodeID() != NodeID.INVALID_ID) {
            m_raftClient.removeFromList("free", new ShortData(nodeID), false);
        }

        // Apply changes
        childs = m_raftClient.readList("new");
        for (RaftData child : childs) {
            StringData data = (StringData) child;
            Gson gson = new Gson();
            NodesConfiguration.NodeEntry node = gson.fromJson(data.getData(), NodesConfiguration.NodeEntry.class);
            nodeID = node.getNodeID();
            m_bloomFilter.add(nodeID);
            node.setStatus(true);
            m_nodes.addNode(node);

            if (nodeID == m_nodes.getOwnNodeID()) {
                // NodeID was already re-used
                m_nodes.setOwnNodeID(NodeID.INVALID_ID);
            }
        }

        if (m_nodes.getOwnNodeID() == NodeID.INVALID_ID) {
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
                // TODO values for backup, capabilities and raft correct?
                NodesConfiguration.NodeEntry node = new NodesConfiguration.NodeEntry(m_ownAddress, nodeID,
                        p_cmdLineRack, p_cmdLineSwitch, p_cmdLineNodeRole, NodeCapabilities.NONE,
                        false, false, true, -1);

                m_nodes.addNode(node);
                Gson gson = new Gson();
                StringData data = new StringData(gson.toJson(node));
                m_nodes.setOwnNodeID(nodeID);
                m_raftClient.addToList("new", data, true);
            } else {
                //get new id by hashing with the local address as seed
                splits = m_ownAddress.getIP().split("\\.");
                seed = (Integer.parseInt(splits[1]) << 16) + (Integer.parseInt(splits[2]) << 8) + Integer.parseInt(
                        splits[3]);
                nodeID = CRC16.continuousHash(seed);
                while (m_bloomFilter.contains(nodeID) || nodeID == NodeID.INVALID_ID) {
                    nodeID = CRC16.continuousHash(--seed);
                }
                m_bloomFilter.add(nodeID);

                // TODO values for backup, capabilities and raft correct?
                NodesConfiguration.NodeEntry node = new NodesConfiguration.NodeEntry(m_ownAddress, nodeID,
                        p_cmdLineRack, p_cmdLineSwitch, p_cmdLineNodeRole, NodeCapabilities.NONE,
                        false, false, true, -1);

                m_nodes.addNode(node);
                // Set own NodeID
                m_nodes.setOwnNodeID(nodeID);
                Gson gson = new Gson();
                StringData data = new StringData(gson.toJson(node));
                m_raftClient.addToList("new", data, true);
            }
        }

        LOGGER.trace("Exiting parseNodes");
        return true;
    }


    @Override
    protected void resolveComponentDependencies(DXRAMComponentAccessor p_componentAccessor) {
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
        initRaft(getConfig().getNodesConfig());

        m_isStarting = true;

        m_nodes = new NodesConfiguration();

        if (!parseNodes(getConfig().getNodesConfig(), role, getConfig().getRack(), getConfig().getSwitch())) {

            LOGGER.error("Parsing nodes failed");

            return false;
        }

        return true;

    }

    private void initRaft(List<NodesConfiguration.NodeEntry> p_nodes) {

        List<RaftAddress> addresses = new ArrayList<>(p_nodes.size());
        for (NodesConfiguration.NodeEntry node: p_nodes) {

            int port = node.getDXRaftPort();

            if (port != -1) {
                addresses.add(new RaftAddress(node.getAddress().getIP(), port));
            }
        }

        RaftContext clientContext = new RaftContext(addresses);
        m_raftClient = new RaftClient(clientContext);
    }
}
