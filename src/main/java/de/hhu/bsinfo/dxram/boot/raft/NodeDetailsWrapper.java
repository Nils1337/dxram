package de.hhu.bsinfo.dxram.boot.raft;

import de.hhu.bsinfo.dxraft.data.DataTypes;
import de.hhu.bsinfo.dxraft.data.RaftData;
import de.hhu.bsinfo.dxram.boot.NodeDetails;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Accessors(prefix = "m_")
public class NodeDetailsWrapper implements RaftData {

    // TODO probably better to make this registration non-static
    private static final byte NODE_DETAILS_TYPE = 51;

    static {
        DataTypes.registerDataType(NODE_DETAILS_TYPE, NodeDetailsWrapper.class);
    }

    private NodeDetails m_nodeDetails;


    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeByte(NODE_DETAILS_TYPE);
        p_exporter.exportObject(m_nodeDetails);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_nodeDetails = new NodeDetails();
        p_importer.importObject(m_nodeDetails);
    }

    @Override
    public int sizeofObject() {
        return Byte.BYTES + m_nodeDetails.sizeofObject();
    }
}
