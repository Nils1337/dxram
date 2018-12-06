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

package de.hhu.bsinfo.dxram.lookup.events;

import de.hhu.bsinfo.dxram.boot.NodeDetails;
import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * An event for node joining
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.03.2017
 * @author Filip Krakowski, Filip.Krakowski@hhu.de, 18.05.2018
 */
@Getter
@Accessors(prefix = {"m_"})
public class NodeJoinEvent extends AbstractEvent {
    private NodeDetails m_nodeDetails;

    /**
     * Creates an instance of NodeJoinEvent
     *
     * @param p_sourceClass
     *         the calling class
     * @param p_nodeDetails
     *          Node details of joining node
     */
    public NodeJoinEvent(final String p_sourceClass, final NodeDetails p_nodeDetails) {
        super(p_sourceClass);
        m_nodeDetails = p_nodeDetails;
    }
}
