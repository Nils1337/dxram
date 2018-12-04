package de.hhu.bsinfo.dxram.boot;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMComponentConfig.Settings(component = BootComponent.class, supportsSuperpeer = true, supportsPeer = true)
public class BootComponentConfig extends DXRAMComponentConfig {

    @Expose
    private String m_consensusProvider = "dxraft";

    /**
     * The rack this node is in. Must be set if node was not in initial nodes file.
     */
    @Expose
    private short m_rack = 0;

    /**
     * The switch this node is connected to. Must be set if node was not in initial nodes file.
     */
    @Expose
    private short m_switch = 0;

    /**
     * Bloom filter size. Bloom filter is used to increase node ID creation performance.
     */
    @Expose
    private StorageUnit m_bitfieldSize = new StorageUnit(2, StorageUnit.MB);

    @Expose
    private boolean m_isClient = false;

    @Expose
    private ZookeeperHandlerConfig m_zookeeperConfig = new ZookeeperHandlerConfig();

    @Expose
    private DXRaftHandlerConfig m_dxraftConfig = new DXRaftHandlerConfig();

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        if (m_bitfieldSize.getBytes() < 2048 * 1024) {
            LOGGER.warn("Bitfield size is rather small. Not all node IDs may be addressable because of high " +
                    "false positives rate!");
        }

        if (p_config.getEngineConfig().getRole() == NodeRole.SUPERPEER && m_isClient) {
            LOGGER.error("Client nodes can't be superpeers");
            return false;
        }

        return true;
    }
}
