/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.net.core.Response;

/**
 * Response to the change size request
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 06.05.2016
 */
public class BarrierChangeSizeResponse extends Response {

    private byte m_status;

    /**
     * Creates an instance of BarrierChangeSizeResponse
     */
    public BarrierChangeSizeResponse() {
        super();

    }

    /**
     * Creates an instance of BarrierChangeSizeReBarrierChangeSizeResponsesponse
     *
     * @param p_request
     *         the corresponding BarrierAllocRequest
     */
    public BarrierChangeSizeResponse(final BarrierChangeSizeRequest p_request, final byte p_status) {
        super(p_request, LookupMessages.SUBTYPE_BARRIER_CHANGE_SIZE_RESPONSE);
        m_status = p_status;
    }

    /**
     * Get the status
     *
     * @return the status
     */
    public int getStatus() {
        return m_status;
    }

    @Override
    protected final int getPayloadLength() {
        return Byte.BYTES;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeByte(m_status);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_status = p_importer.readByte(m_status);
    }
}
