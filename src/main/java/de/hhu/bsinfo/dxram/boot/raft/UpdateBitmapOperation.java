package de.hhu.bsinfo.dxram.boot.raft;

import de.hhu.bsinfo.dxraft.client.atomic.AtomicOperation;
import de.hhu.bsinfo.dxraft.client.atomic.AtomicOperationFactory;
import de.hhu.bsinfo.dxraft.data.BooleanData;
import de.hhu.bsinfo.dxraft.data.RaftData;
import de.hhu.bsinfo.dxraft.state.RaftEntry;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
public class UpdateBitmapOperation implements AtomicOperation {
    private static final byte UPDATE_BITMAP_TYPE = 50;

    static {
        AtomicOperationFactory.registerAtomicOperationType(UPDATE_BITMAP_TYPE, UpdateBitmapOperation.class);
    }

    private short m_id;

    @Override
    public RaftData apply(RaftEntry p_raftEntry) {
        RaftData data = p_raftEntry.getData();
        if (data instanceof Bitmap) {
            Bitmap bitmap = (Bitmap) data;
            if (!bitmap.isSet(m_id)) {
                bitmap.set(m_id, true);
                return new BooleanData(true);
            }
        }
        return new BooleanData(false);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeByte(UPDATE_BITMAP_TYPE);
        p_exporter.writeShort(m_id);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_id = p_importer.readShort(m_id);
    }

    @Override
    public int sizeofObject() {
        return Byte.BYTES + Short.BYTES;
    }
}
