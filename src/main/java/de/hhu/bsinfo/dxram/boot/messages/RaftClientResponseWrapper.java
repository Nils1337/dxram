package de.hhu.bsinfo.dxram.boot.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class RaftClientResponseWrapper extends Response {
    private byte[] m_msgData;

    public RaftClientResponseWrapper(Request p_request, byte p_subtype, byte[] m_msgData) {
        super(p_request, p_subtype);
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
