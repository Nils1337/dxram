package de.hhu.bsinfo.dxram.boot;

import java.util.Collection;

public interface ConsensusHandler {
    NodeDetails getBootstrapDetails();

    void freeNodeId(short p_id);

    void close();

    boolean start(final NodeDetails p_details) throws Exception;
}
