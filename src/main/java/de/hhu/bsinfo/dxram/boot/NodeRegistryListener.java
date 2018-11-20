package de.hhu.bsinfo.dxram.boot;

public interface NodeRegistryListener {
    void onPeerJoined(final NodeDetails p_nodeDetails);

    void onPeerLeft(final NodeDetails p_nodeDetails);

    void onSuperpeerJoined(final NodeDetails p_nodeDetails);

    void onSuperpeerLeft(final NodeDetails p_nodeDetails);

    void onNodeUpdated(final NodeDetails p_nodeDetails);
}
