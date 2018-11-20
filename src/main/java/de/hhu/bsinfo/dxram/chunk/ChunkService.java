/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.chunk.operation.CIDStatus;
import de.hhu.bsinfo.dxram.chunk.operation.Create;
import de.hhu.bsinfo.dxram.chunk.operation.Get;
import de.hhu.bsinfo.dxram.chunk.operation.Put;
import de.hhu.bsinfo.dxram.chunk.operation.Remove;
import de.hhu.bsinfo.dxram.chunk.operation.Resize;
import de.hhu.bsinfo.dxram.chunk.operation.Status;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

/**
 * Main service for using the key-value store with chunks
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class ChunkService extends AbstractDXRAMService<ChunkServiceConfig> {
    // component dependencies
    private BootComponent m_boot;
    private BackupComponent m_backup;
    private ChunkComponent m_chunk;
    private NetworkComponent m_network;
    private LookupComponent m_lookup;
    private NameserviceComponent m_nameservice;

    // chunk operations of service
    private Status m_status;
    private CIDStatus m_cidStatus;
    private Create m_create;
    private Get m_get;
    private Put m_put;
    private Remove m_remove;
    private Resize m_resize;

    /**
     * Constructor
     */
    public ChunkService() {
        super("chunk", ChunkServiceConfig.class);
    }

    /**
     * Get the status operation
     *
     * @return Operation
     */
    public Status status() {
        return m_status;
    }

    /**
     * Get the cidStatus operation
     *
     * @return Operation
     */
    public CIDStatus cidStatus() {
        return m_cidStatus;
    }

    /**
     * Get the create operation
     *
     * @return Operation
     */
    public Create create() {
        return m_create;
    }

    /**
     * Get the get operation
     *
     * @return Operation
     */
    public Get get() {
        return m_get;
    }

    /**
     * Get the put operation
     *
     * @return Operation
     */
    public Put put() {
        return m_put;
    }

    /**
     * Get the remove operation
     *
     * @return Operation
     */
    public Remove remove() {
        return m_remove;
    }

    /**
     * Get the resize operation
     *
     * @return Operation
     */
    public Resize resize() {
        return m_resize;
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
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_nameservice = p_componentAccessor.getComponent(NameserviceComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.Config p_config) {
        m_status = new Status(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);
        m_cidStatus = new CIDStatus(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);
        m_create = new Create(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);
        m_get = new Get(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);
        m_put = new Put(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);
        m_remove = new Remove(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice,
                p_config.getServiceConfig(ChunkServiceConfig.class).getRemoverQueueSize());
        m_resize = new Resize(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }
}
