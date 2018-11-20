package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Component for managing the (nameservice) index which is stored as a chunk
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class ChunkIndexComponent extends AbstractDXRAMComponent<ChunkIndexComponentConfig> {
    // component dependencies
    private BackupComponent m_backup;
    private BootComponent m_boot;
    private ChunkComponent m_chunk;
    private NetworkComponent m_network;

    /**
     * Constructor
     */
    public ChunkIndexComponent() {
        super(DXRAMComponentOrder.Init.CHUNK_INDEX, DXRAMComponentOrder.Shutdown.CHUNK_INDEX,
                ChunkIndexComponentConfig.class);
    }

    /**
     * Create index chunk for the nameservice.
     *
     * @param p_size
     *         Size of the index chunk.
     * @return Chunkid of the index chunk.
     */
    public long createIndexChunk(final int p_size) {
        return m_chunk.getMemory().create().create(p_size);
    }

    /**
     * Register index chunk for the nameservice. Is called for the first nameservice entry added.
     *
     * @param p_chunkID
     *         the ChunkID of the nameservice index chunk
     * @param p_size
     *         Size of the index chunk.
     */
    public void registerIndexChunk(final long p_chunkID, final int p_size) {
        if (p_chunkID != ChunkID.INVALID_ID) {
            m_backup.registerChunk(p_chunkID, p_size);
        }
    }

    /**
     * Internal chunk put for index data.
     *
     * @param p_chunk
     *         Data structure to put
     * @return True if successful, false otherwise
     */
    public boolean putIndexChunk(final AbstractChunk p_chunk) {
        m_chunk.getMemory().put().put(p_chunk, ChunkLockOperation.ACQUIRE_OP_RELEASE, -1);

        if (!p_chunk.isStateOk()) {
            return false;
        }

        if (m_backup.isActive()) {
            BackupRange backupRange = m_backup.getBackupRange(p_chunk.getID());
            BackupPeer[] backupPeers = backupRange.getBackupPeers();

            if (backupPeers != null) {
                for (BackupPeer peer : backupPeers) {
                    if (peer != null && peer.getNodeID() != m_boot.getNodeId()) {

                        LOGGER.trace("Logging 0x%x to %s", p_chunk.getID(), NodeID.toHexString(peer.getNodeID()));

                        try {
                            m_network.sendMessage(new LogMessage(peer.getNodeID(), backupRange.getRangeID(), p_chunk));
                        } catch (final NetworkException ignore) {

                        }
                    }
                }
            }
        }

        return true;
    }

    @Override
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(BootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_chunk = p_componentAccessor.getComponent(ChunkComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config, final DXRAMJNIManager p_jniManager) {
        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        return true;
    }
}
