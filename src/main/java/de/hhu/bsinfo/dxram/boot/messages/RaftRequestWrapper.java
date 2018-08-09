package de.hhu.bsinfo.dxram.boot.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class RaftRequestWrapper extends Request {
    private byte[] m_msgData;

    public RaftRequestWrapper(short p_destination, byte p_type, byte p_subtype, byte[] m_msgData) {
        super(p_destination, p_type, p_subtype, true);
        this.m_msgData = m_msgData;
    }

    public byte[] getMsgData() {
        return m_msgData;
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_msgData = p_importer.readByteArray(m_msgData);
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofByteArray(m_msgData);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeByteArray(m_msgData);
    }
}
