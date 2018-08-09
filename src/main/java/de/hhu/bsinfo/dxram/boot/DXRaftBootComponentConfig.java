package de.hhu.bsinfo.dxram.boot;

import java.util.ArrayList;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;

import static de.hhu.bsinfo.dxram.util.NodeCapabilities.COMPUTE;
import static de.hhu.bsinfo.dxram.util.NodeCapabilities.NONE;
import static de.hhu.bsinfo.dxram.util.NodeCapabilities.STORAGE;
import static de.hhu.bsinfo.dxram.util.NodeCapabilities.toMask;

public class DXRaftBootComponentConfig extends AbstractDXRAMComponentConfig {

    @Expose
    private int port = 6653;

    @Expose
    private StorageUnit m_bitfieldSize = new StorageUnit(2, StorageUnit.MB);

    @Expose
    private ArrayList<NodesConfiguration.NodeEntry> m_nodesConfig = new ArrayList<NodesConfiguration.NodeEntry>() {
        {
            // default values for local testing
            add(new NodesConfiguration.NodeEntry(new IPV4Unit("127.0.0.1", 22221), NodeID.INVALID_ID, (short) 0,
                    (short) 0, NodeRole.SUPERPEER, NONE, true, true, false));
            add(new NodesConfiguration.NodeEntry(new IPV4Unit("127.0.0.1", 22222), NodeID.INVALID_ID, (short) 0,
                    (short) 0, NodeRole.PEER, toMask(STORAGE, COMPUTE), true, true, false));
            add(new NodesConfiguration.NodeEntry(new IPV4Unit("127.0.0.1", 22223), NodeID.INVALID_ID, (short) 0,
                    (short) 0, NodeRole.PEER, toMask(STORAGE, COMPUTE), true, true, false));
        }
    };

    @Expose
    private short m_rack = 0;

    @Expose
    private short m_switch = 0;

    /**
     * Constructor
     */
    public DXRaftBootComponentConfig() {
        super(DXRaftBootComponent.class, true, true);
    }

    /**
     * The rack this node is in. Must be set if node was not in initial nodes file.
     */
    public short getRack() {
        return m_rack;
    }

    /**
     * The switch this node is connected to. Must be set if node was not in initial nodes file.
     */
    public short getSwitch() {
        return m_switch;
    }

    /**
     * Port the raft server should listen to
     */
    public int getPort() {
        return port;
    }

    /**
     * Bloom filter size. Bloom filter is used to increase node ID creation performance.
     */
    public StorageUnit getBitfieldSize() {
        return m_bitfieldSize;
    }

    /**
     * Nodes configuration
     * We can't use the NodesConfiguration class with the configuration because the nodes in that class
     * are already mapped to their node ids
     */
    public ArrayList<NodesConfiguration.NodeEntry> getNodesConfig() {
        return m_nodesConfig;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        if (m_bitfieldSize.getBytes() < 2048 * 1024) {

            LOGGER.warn("Bitfield size is rather small. Not all node IDs may be addressable because of high " +
                    "false positives rate!");

        }

        return true;
    }
}
