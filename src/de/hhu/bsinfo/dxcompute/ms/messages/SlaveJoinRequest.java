
package de.hhu.bsinfo.dxcompute.ms.messages;

import de.hhu.bsinfo.dxcompute.DXCOMPUTEMessageTypes;
import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Request of the slave to a master to join a compute group.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class SlaveJoinRequest extends AbstractRequest {
	/**
	 * Creates an instance of SlaveJoinRequest.
	 * This constructor is used when receiving this message.
	 */
	public SlaveJoinRequest() {
		super();
	}

	/**
	 * Creates an instance of SlaveJoinRequest.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public SlaveJoinRequest(final short p_destination) {
		super(p_destination, DXCOMPUTEMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
				MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_REQUEST);
	}
}
