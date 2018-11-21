package de.hhu.bsinfo.dxram.boot;

import de.hhu.bsinfo.dxram.util.NodeRole;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class NodeRegistry {

    // All known nodes
    private List<NodeDetails> m_nodes;
    private NodeRegistryListener m_listener;

    public void addNewNode(NodeDetails p_nodeDetails) {
        m_nodes.add(p_nodeDetails);
        if (m_listener != null) {
            if (p_nodeDetails.getRole() == NodeRole.PEER) {
                m_listener.onPeerJoined(p_nodeDetails);
            } else if (p_nodeDetails.getRole() == NodeRole.SUPERPEER) {
                m_listener.onSuperpeerJoined(p_nodeDetails);
            }
        }
    }

    public void removeNode(short p_id) {
        Optional<NodeDetails> details = m_nodes.stream().filter(node -> node.getId() == p_id).findAny();
        m_nodes = m_nodes.stream().filter(node -> node.getId() != p_id).collect(Collectors.toList());
        if (details.isPresent() && m_listener != null) {
            if (details.get().getRole() == NodeRole.PEER) {
                m_listener.onPeerJoined(details.get());
            } else if (details.get().getRole() == NodeRole.SUPERPEER) {
                m_listener.onSuperpeerJoined(details.get());
            }
        }
    }

    public NodeDetails getDetails(short p_id) {
        Optional<NodeDetails> nodeDetails = m_nodes.stream().filter(node -> node.getId() == p_id).findAny();
        if (nodeDetails.isPresent()) {
            return nodeDetails.get();
        }
        return null;
    }

    public List<NodeDetails> getAll() {
        return m_nodes;
    }

    public void registerRegistryListener(NodeRegistryListener p_listener) {
        m_listener = p_listener;
    }
}
