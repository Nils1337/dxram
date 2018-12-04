package de.hhu.bsinfo.dxram.boot;

import lombok.Data;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Config for the ZookeeperHandler
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
public class ZookeeperHandlerConfig {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ZookeeperHandlerConfig.class);

    /**
     * Path for zookeeper entry
     */
    @Expose
    private String m_path = "/dxram";

    /**
     * Address and port of zookeeper
     */
    @Expose
    private IPV4Unit m_connection = new IPV4Unit("127.0.0.1", 2181);

    @Expose
    private TimeUnit m_timeout = new TimeUnit(10, TimeUnit.SEC);
}
