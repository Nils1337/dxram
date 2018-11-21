package de.hhu.bsinfo.dxram.boot;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMComponentConfig.Settings(component = BootComponent.class, supportsSuperpeer = true, supportsPeer = true)
public class BootComponentConfig extends DXRAMComponentConfig {

    @Expose
    private String m_nodeRegistry = "dxraft";

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

    @Expose
    private boolean m_isClient = false;

    @Expose
    private ZookeeperHandlerConfig m_zookeeperConfig = new ZookeeperHandlerConfig();

    @Expose
    private DXRaftHandlerConfig m_dxraftConfig = new DXRaftHandlerConfig();
}
