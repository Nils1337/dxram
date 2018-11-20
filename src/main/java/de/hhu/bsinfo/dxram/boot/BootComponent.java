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
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
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
public class BootComponent extends AbstractDXRAMComponent<BootComponentConfig> {
    private static final InetSocketAddress INVALID_ADDRESS = new InetSocketAddress("255.255.255.255", 0xFFFF);

    private DXRAMContext.Config m_contextConfig;

    private short m_id = NodeID.INVALID_ID;
    private String m_address;
    private int m_port;
    private NodeRole m_role;
    private NodeRegistry m_nodeRegistry;

    /**
     * Constructor
     */
    protected BootComponent() {
        super(DXRAMComponentOrder.Init.BOOT, DXRAMComponentOrder.Shutdown.BOOT, BootComponentConfig.class);
    }

    @Override
    protected boolean initComponent(DXRAMContext.Config p_config, DXRAMJNIManager p_jniManager) {
        m_contextConfig = p_config;
        m_address = m_contextConfig.getEngineConfig().getAddress().getIP();
        m_port = m_contextConfig.getEngineConfig().getAddress().getPort();
        m_role = m_contextConfig.getEngineConfig().getRole();

        if (m_config.getNodeRegistry().equals("dxnet")) {
            m_nodeRegistry = new DXRaftNodeRegistry(m_config.getDxraftConfig());
        } else {
            m_nodeRegistry = new ZookeeperNodeRegistry(m_config.getZookeeperConfig());
        }

        try {
            m_nodeRegistry.start(buildNodeDetails());
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
        m_nodeRegistry.close();
        return true;
    }

    /**
     * Builds this node's details.
     *
     * @return This node's details.
     */
    protected NodeDetails buildNodeDetails() {
        return NodeDetails.builder(NodeID.INVALID_ID, m_address, m_port)
                .withRole(m_role)
                .withRack(m_config.getRack())
                .withSwitch(m_config.getSwitch())
                .withOnline(true)
                .withCapabilities(detectNodeCapabilities())
                .build();
    }

    /**
     * Detects this node's capabilities.
     *
     * @return This node's capabilities.
     */
    private int detectNodeCapabilities() {
        if (m_role == NodeRole.SUPERPEER) {
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
     * Register a node registry listener
     *
     * @param p_listener
     *         Listener to register
     */
    public void registerRegistryListener(final NodeRegistryListener p_listener) {
        m_nodeRegistry.registerListener(p_listener);
    }

    /**
     * Returns this node's details.
     *
     * @return This node's details.
     */
    public NodeDetails getDetails() {
        return m_nodeRegistry.getDetails();
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

    /**
     * Updates this node's capabilities.
     *
     * @param p_capabilities
     *         The updated capabilities.
     */
    public void updateNodeCapabilities(int p_capabilities) {
        NodeDetails oldDetails = m_nodeRegistry.getDetails();

        if (oldDetails == null) {
            throw new IllegalStateException("Lost own node information");
        }

        m_nodeRegistry.updateNodeDetails(oldDetails.withCapabilities(p_capabilities));
    }

    /**
     * Get the node ID of the currently set bootstrap node.
     *
     * @return Node ID assigned for bootstrapping or -1 if no bootstrap assigned/available.
     */
    public short getBootstrapId() {
        NodeDetails bootstrapDetails = m_nodeRegistry.getBootstrapDetails();

        if (bootstrapDetails == null) {
            return NodeID.INVALID_ID;
        }

        return bootstrapDetails.getId();
    }

    public short getNodeId() {
        return getDetails().getId();
    }

    public NodeRole getNodeRole() {
        return getDetails().getRole();
    }

    public short getRack() {
        return getDetails().getRack();
    }

    public short getSwitch() {
        return getDetails().getSwitch();
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
}
