package de.hhu.bsinfo.dxram.boot;

import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;

import java.util.*;
import java.util.stream.Collectors;

public class NodeRegistry {

    // All known nodes
    private NodeDetails[] m_nodes = new NodeDetails[NodeID.MAX_ID + 1];

    private NodeRegistryListener m_listener;

    public synchronized void addNewNode(NodeDetails p_nodeDetails) {
        short nodeID = p_nodeDetails.getId();
        NodeDetails prev = m_nodes[nodeID & 0xFFFF];
        m_nodes[nodeID & 0xFFFF] = p_nodeDetails;

        if (m_listener != null && prev == null || !prev.getAddress().equals(p_nodeDetails.getAddress())) {
            if (p_nodeDetails.getRole() == NodeRole.PEER) {
                m_listener.onPeerJoined(p_nodeDetails);
            } else if (p_nodeDetails.getRole() == NodeRole.SUPERPEER) {
                m_listener.onSuperpeerJoined(p_nodeDetails);
            }
        }
    }

    public synchronized void removeNode(short p_id) {
        NodeDetails prev = m_nodes[p_id & 0xFFFF];
        m_nodes[p_id & 0xFFFF] = null;
        if (m_listener != null && prev != null) {
            if (prev.getRole() == NodeRole.PEER) {
                m_listener.onPeerJoined(prev);
            } else if (prev.getRole() == NodeRole.SUPERPEER) {
                m_listener.onSuperpeerJoined(prev);
            }
        }
    }

    public NodeDetails getDetails(short p_id) {
        return m_nodes[p_id & 0xFFFF];
    }

    public List<NodeDetails> getAll() {
        return Arrays.stream(m_nodes).filter(details -> details != null).collect(Collectors.toList());
    }

    public void registerRegistryListener(NodeRegistryListener p_listener) {
        m_listener = p_listener;
    }
}
