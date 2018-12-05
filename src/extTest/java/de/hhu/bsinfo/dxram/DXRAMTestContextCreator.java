package de.hhu.bsinfo.dxram;

import de.hhu.bsinfo.dxram.backup.BackupComponentConfig;
import de.hhu.bsinfo.dxram.boot.BootComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentManager;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMContextCreator;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceManager;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

/**
 * Creates a DXRAMContext for the DXRAM runner. This allows runtime configuration of settings for the DXRAM
 * instances to start for the tests to run.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
class DXRAMTestContextCreator implements DXRAMContextCreator {
    private static final int DXRAM_NODE_PORT_START = 22221;
    private static final int DXRAFT_INTERNAL_PORT_START = 5454;
    private static final int DXRAFT_REQUEST_PORT_START = 5000;
    private static final int DXRAFT_CLIENT_PORT_START = 6000;

    private final DXRAMTestConfiguration m_config;
    private final int m_nodeIdx;
    private final IPV4Unit m_zookeeperConnection;

    /**
     * Constructor
     *
     * @param p_config
     *         Configuration for the test class to run
     * @param p_nodeIdx
     *         Index of node to configur
     */
    DXRAMTestContextCreator(final DXRAMTestConfiguration p_config, IPV4Unit p_zookeeperConnection,
            final int p_nodeIdx) {
        m_config = p_config;
        m_zookeeperConnection = p_zookeeperConnection;
        m_nodeIdx = p_nodeIdx;
    }

    @Override
    public DXRAMContext create(final DXRAMComponentManager p_componentManager,
            final DXRAMServiceManager p_serviceManager) {
        DXRAMContext context = new DXRAMContext();
        context.createDefaultComponents(p_componentManager);
        context.createDefaultServices(p_serviceManager);

        context.getConfig().getEngineConfig().setRole(m_config.nodes()[m_nodeIdx].nodeRole().toString());
        context.getConfig().getEngineConfig().setAddress(new IPV4Unit("127.0.0.1", DXRAM_NODE_PORT_START + m_nodeIdx));

        //context.getConfig().getEngineConfig().setJniPath("/usr/lib/jni");

        BootComponentConfig bootConfig = context.getConfig().getComponentConfig(BootComponentConfig.class);
        bootConfig.setConsensusProvider(m_config.consensusProvider());

        if ("zookeeper".equals(m_config.consensusProvider())) {
            bootConfig.getZookeeperConfig().setConnection(
                    m_zookeeperConnection);
        } else {
            if (m_nodeIdx == 0) {
                bootConfig.getDxraftConfig().setBootstrapPeer(true);
            } else {
                bootConfig.getDxraftConfig().setBootstrapPeer(false);
            }

            bootConfig.getDxraftConfig().getRaftServerConfig().setRequestPort(DXRAFT_REQUEST_PORT_START + m_nodeIdx);
            bootConfig.getDxraftConfig().getRaftServerConfig().setInternalPort(DXRAFT_INTERNAL_PORT_START + m_nodeIdx);
            bootConfig.getDxraftConfig().getRaftClientConfig().setPort(DXRAFT_CLIENT_PORT_START + m_nodeIdx);
        }

        context.getConfig().getComponentConfig(BackupComponentConfig.class).setBackupActive(
                m_config.nodes()[m_nodeIdx].backupActive());
        context.getConfig().getComponentConfig(BackupComponentConfig.class).setAvailableForBackup(
                m_config.nodes()[m_nodeIdx].availableForBackup());

        return context;
    }
}
