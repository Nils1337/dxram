package de.hhu.bsinfo.dxram.boot;

import java.util.Collection;

public interface NodeRegistry {
    void registerListener(final NodeRegistryListener p_listener);

    NodeDetails getDetails();

    Collection<NodeDetails> getAll();

    NodeDetails getDetails(final short p_nodeId);

    NodeDetails getBootstrapDetails();

    void updateNodeDetails(NodeDetails p_details);

    void close();

    boolean start(final NodeDetails p_details) throws Exception;
}
