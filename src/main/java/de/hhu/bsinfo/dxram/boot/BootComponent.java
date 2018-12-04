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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupComponentConfig;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.chunk.ChunkComponentConfig;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.lookup.events.NodeJoinEvent;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Component executing the bootstrapping of a node in DXRAM.
 * It takes care of assigning the node ID to this node, its role and
 * managing everything related to the basic node status (available,
 * failure report...)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 * @author Filip Krakowski, Filip.Krakowski@hhu.de, 18.05.2018
 */
public class BootComponent extends AbstractDXRAMComponent<BootComponentConfig> implements EventListener<AbstractEvent> {
    private static final InetSocketAddress INVALID_ADDRESS = new InetSocketAddress("255.255.255.255", 0xFFFF);

    private DXRAMContext.Config m_contextConfig;

    private short m_id = NodeID.INVALID_ID;
    private String m_address;
    private int m_port;
    private NodeRole m_role;
    private ConsensusHandler m_consensusHandler;
    private NodeRegistry m_nodeRegistry;
    private NodeDetails m_nodeDetails;
    private EventComponent m_eventComponent;

    @Override
    protected void resolveComponentDependencies(DXRAMComponentAccessor p_componentAccessor) {
        m_eventComponent = p_componentAccessor.getComponent(EventComponent.class);
    }

    /**
     * Constructor
     */
    public BootComponent() {
        super(DXRAMComponentOrder.Init.BOOT, DXRAMComponentOrder.Shutdown.BOOT, BootComponentConfig.class);
    }

    @Override
    protected boolean initComponent(DXRAMContext.Config p_config, DXRAMJNIManager p_jniManager) {
        m_contextConfig = p_config;
        m_nodeDetails = buildNodeDetails();
        m_nodeRegistry = new NodeRegistry();
        m_nodeRegistry.addNewNode(m_nodeDetails);
        m_eventComponent.registerListener(this, NodeJoinEvent.class);
        m_eventComponent.registerListener(this, NodeFailureEvent.class);

        if ("dxraft".equals(m_config.getConsensusProvider())) {
            m_consensusHandler = new DXRaftHandler(m_config);
        } else {
            m_consensusHandler = new ZookeeperHandler(m_config.getZookeeperConfig());
        }

        try {
            m_consensusHandler.start(m_nodeDetails);
        } catch (Exception e) {
            LOGGER.error("Starting node registry failed", e);
            return false;
        }

        return true;
    }

    /**
     * Check if the component supports the superpeer node role.
     *
     * @return True if supporting, false otherwise.
     */
    @Override
    protected boolean supportsSuperpeer() {
        return true;
    }

    /**
     * Check if the component supports the peer node role.
     *
     * @return True if supporting, false otherwise.
     */
    @Override
    protected boolean supportsPeer() {
        return true;
    }

    /**
     * Shut down this component.
     *
     * @return True if shutting down was successful, false otherwise.
     */
    @Override
    protected boolean shutdownComponent() {
        m_consensusHandler.close();
        return true;
    }

    /**
     * Builds this node's details.
     *
     * @return This node's details.
     */
    private NodeDetails buildNodeDetails() {
        return NodeDetails.builder(NodeID.INVALID_ID, m_contextConfig.getEngineConfig().getAddress().getIP(),
                m_contextConfig.getEngineConfig().getAddress().getPort())
                .withRole(m_contextConfig.getEngineConfig().getRole())
                .withRack(m_config.getRack())
                .withSwitch(m_config.getSwitch())
                .withOnline(true)
                .withCapabilities(detectNodeCapabilities(m_contextConfig.getEngineConfig().getRole()))
                .build();
    }

    /**
     * Detects this node's capabilities.
     *
     * @return This node's capabilities.
     */
    private int detectNodeCapabilities(NodeRole p_role) {
        if (p_role == NodeRole.SUPERPEER) {
            return NodeCapabilities.NONE;
        }

        if (m_config.isClient()) {
            return NodeCapabilities.COMPUTE;
        }

        ChunkComponentConfig chunkConfig = m_contextConfig.getComponentConfig(ChunkComponentConfig.class);
        BackupComponentConfig backupConfig = m_contextConfig.getComponentConfig(BackupComponentConfig.class);

        int capabilities = 0;

        if (chunkConfig.isChunkStorageEnabled()) {
            capabilities |= NodeCapabilities.STORAGE;
        }

        if (backupConfig.isBackupActive()) {
            capabilities |= NodeCapabilities.BACKUP_SRC;
        }

        if (backupConfig.isBackupActive() && backupConfig.isAvailableForBackup()) {
            capabilities |= NodeCapabilities.BACKUP_DST;
        }

        LOGGER.info("Detected capabilities %s", NodeCapabilities.toString(capabilities));

        return capabilities;
    }

    /**
     * Returns this node's details.
     *
     * @return This node's details.
     */
    public NodeDetails getDetails() {
        return m_nodeDetails;
    }

    /**
     * Returns the specified node's details.
     *
     * @return This specified node's details.
     */
    public NodeDetails getDetails(short p_nodeId) {
        return m_nodeRegistry.getDetails(p_nodeId);
    }

    /**
     * Get node entries of all available (online) nodes including the own.
     *
     * @return List of IDs of nodes available.
     */
    public List<NodeDetails> getOnlineNodes() {
        return m_nodeRegistry.getAll().stream()
                .filter(NodeDetails::isOnline)
                .collect(Collectors.toList());
    }

    /**
     * Get IDs of all available (online) nodes including the own.
     *
     * @return List of IDs of nodes available.
     */
    public List<Short> getOnlineNodeIds() {
        return m_nodeRegistry.getAll().stream()
                .filter(NodeDetails::isOnline)
                .map(NodeDetails::getId)
                .collect(Collectors.toList());
    }

    /**
     * Get IDs of all available (online) peer nodes except the own.
     *
     * @return List of IDs of nodes available without own ID.
     */
    public List<Short> getOnlinePeerIds() {
        return m_nodeRegistry.getAll().stream()
                .filter(NodeDetails::isOnline)
                .filter(node -> node.getRole() == NodeRole.PEER)
                .map(NodeDetails::getId)
                .collect(Collectors.toList());
    }

    /**
     * Get IDs of all available (online) superpeer nodes except the own.
     *
     * @return List of IDs of nodes available without own ID.
     */
    public List<Short> getOnlineSuperpeerIds() {
        return m_nodeRegistry.getAll().stream()
                .filter(NodeDetails::isOnline)
                .filter(node -> node.getRole() == NodeRole.SUPERPEER)
                .map(NodeDetails::getId)
                .collect(Collectors.toList());
    }

    /**
     * Get IDs of all available (online) backup peers.
     *
     * @return List of IDs of peers available for backup without own ID.
     */
    public List<BackupPeer> getAvailableBackupPeers() {
        return m_nodeRegistry.getAll().stream()
                .filter(node -> node.getRole() == NodeRole.PEER && node.isOnline() && node.isAvailableForBackup())
                .map(node -> new BackupPeer(node.getId(), node.getRack(), node.getSwitch()))
                .collect(Collectors.toList());
    }

    /**
     * Collects all node ids supporting the specified capabilities.
     *
     * @param p_capabilities
     *         The requested capabilities.
     * @return A list containing all matching node ids.
     */
    public List<Short> getSupportingNodes(final int p_capabilities) {
        return m_nodeRegistry.getAll().stream()
                .filter(node -> NodeCapabilities.supportsAll(node.getCapabilities(), p_capabilities))
                .map(NodeDetails::getId)
                .collect(Collectors.toList());
    }

    public void addNodeToRegistry(NodeDetails p_nodeDetails) {
        m_nodeRegistry.addNewNode(p_nodeDetails);
    }

    public void removeNodeFromRegistry(short p_id) {
        m_nodeRegistry.removeNode(p_id);
    }

    /**
     * Updates this node's capabilities.
     *
     * @param p_capabilities
     *         The updated capabilities.
     */
    public void updateNodeCapabilities(int p_capabilities) {
//        NodeDetails oldDetails = m_consensusHandler.getDetails();
//
//        if (oldDetails == null) {
//            throw new IllegalStateException("Lost own node information");
//        }
//
//        m_consensusHandler.updateNodeDetails(oldDetails.withCapabilities(p_capabilities));
    }

//    /**
//     * Get the node ID of the currently set bootstrap node.
//     *
//     * @return Node ID assigned for bootstrapping or -1 if no bootstrap assigned/available.
//     */
//    public short getBootstrapId() {
//        NodeDetails bootstrapDetails = m_consensusHandler.getBootstrapDetails();
//
//        if (bootstrapDetails == null) {
//            return NodeID.INVALID_ID;
//        }
//
//        return bootstrapDetails.getId();
//    }

    /**
     * Get the node details of the currently set bootstrap node.
     *
     * @return Node details assigned for bootstrapping or null if no bootstrap assigned/available.
     */
    public NodeDetails getBootstrapDetails() {
        return m_consensusHandler.getBootstrapDetails();
    }


    public short getNodeId() {
        return m_nodeDetails.getId();
    }

    public NodeRole getNodeRole() {
        return m_nodeDetails.getRole();
    }

    public short getRack() {
        return m_nodeDetails.getRack();
    }

    public short getSwitch() {
        return m_nodeDetails.getSwitch();
    }

    public int getNumberOfAvailableSuperpeers() {
        return getOnlineSuperpeerIds().size();
    }

    public InetSocketAddress getNodeAddress(short p_nodeId) {
        NodeDetails details = getDetails(p_nodeId);

        if (details == null) {
            LOGGER.warn("Couldn't find node 0x%04X", p_nodeId);
            return INVALID_ADDRESS;
        }

        return details.getAddress();
    }

    public NodeRole getNodeRole(short p_nodeId) {
        NodeDetails details = getDetails(p_nodeId);

        if (details == null) {
            LOGGER.warn("Couldn't find node 0x%04X", p_nodeId);
            return null;
        }

        return details.getRole();
    }

    public boolean isNodeOnline(short p_nodeId) {
        NodeDetails details = getDetails(p_nodeId);

        if (details == null) {
            LOGGER.warn("Couldn't find node 0x%04X", p_nodeId);
            return false;
        }

        return details.isOnline();
    }

    public int getNodeCapabilities(short p_nodeId) {
        NodeDetails details = getDetails(p_nodeId);

        if (details == null) {
            LOGGER.warn("Couldn't find node 0x%04X", p_nodeId);
            return NodeCapabilities.INVALID;
        }

        return details.getCapabilities();
    }

    public void registerRegistryListener(NodeRegistryListener p_listener) {
        m_nodeRegistry.registerRegistryListener(p_listener);
    }

    @Override
    public void eventTriggered(AbstractEvent p_event) {
        if (p_event instanceof NodeJoinEvent) {
            NodeJoinEvent joinEvent = (NodeJoinEvent) p_event;
            NodeDetails newNode = NodeDetails.builder(joinEvent.getNodeID(), joinEvent.getAddress().getIP(),
                    joinEvent.getAddress().getPort())
                    .withAvailableForBackup(joinEvent.isAvailableForBackup())
                    .withCapabilities(joinEvent.getCapabilities())
                    .withOnline(true)
                    .withRack(joinEvent.getRack())
                    .withSwitch(joinEvent.getSwitch())
                    .withRole(joinEvent.getRole())
                    .build();
            m_nodeRegistry.addNewNode(newNode);
        } else if (p_event instanceof NodeFailureEvent) {
            NodeFailureEvent failureEvent = (NodeFailureEvent) p_event;
            m_nodeRegistry.removeNode(failureEvent.getNodeID());
            // TODO this should only do one node
            m_consensusHandler.freeNodeId(failureEvent.getNodeID());
        }
    }
}
