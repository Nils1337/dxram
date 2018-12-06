/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.NodeDetails;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * Peer Join Event Request. Request to propagate a peer joining to all other peers (two-phase: 1. inform all superpeers 2. superpeers inform peers).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 03.04.2017
 * @author Filip Krakowski, Filip.Krakowski@hhu.de, 18.05.2018
 */
@Getter
@Accessors(prefix = {"m_"})
public class NodeJoinEventRequest extends Request {

    private NodeDetails m_nodeDetails;

    // Constructors

    /**
     * Creates an instance of NodeJoinEventRequest
     */
    public NodeJoinEventRequest() {
        super();
    }

    /**
     * Creates an instance of NodeJoinEventRequest
     *
     * @param p_destination
     *         the destination
     * @param p_nodeDetails
     *          Node details of joining node
     */
    public NodeJoinEventRequest(final short p_destination, NodeDetails p_nodeDetails) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_NODE_JOIN_EVENT_REQUEST);
        m_nodeDetails = p_nodeDetails;
    }


    @Override
    protected final int getPayloadLength() {
        return m_nodeDetails.sizeofObject();
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.exportObject(m_nodeDetails);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_nodeDetails = new NodeDetails();
        p_importer.importObject(m_nodeDetails);
    }

}
