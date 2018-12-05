package de.hhu.bsinfo.dxram.boot.raft;

import de.hhu.bsinfo.dxraft.data.DataTypes;
import de.hhu.bsinfo.dxraft.data.RaftData;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

@NoArgsConstructor
public class Bitmap implements RaftData, Iterable<Short> {
    private static final byte BITMAP_TYPE = 50;

    static {
        DataTypes.registerDataType(BITMAP_TYPE, Bitmap.class);
    }

    private byte[] m_data;
    private int m_size;

    public Bitmap(int p_size) {
        int size = (int) Math.ceil((double) p_size /8);
        m_data = new byte[size];
    }

    public boolean isSet(short p_idx) {
        int id = p_idx >= 0 ? p_idx : -p_idx + Short.MAX_VALUE;
        int idx = id / 8;
        int shift = id % 8;
        int bit = m_data[idx] >> shift & 1;
        return bit == 1;
    }

    public void set(short p_idx, boolean p_value) {
        int id = p_idx >= 0 ? p_idx : -p_idx + Short.MAX_VALUE;
        int shift = id % 8;
        int idx = id / 8;
        if (p_value) {
            m_data[idx] |= 1 << shift;
        } else {
            m_data[idx] &= ~(1 << shift);
        }
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeByte(BITMAP_TYPE);
        p_exporter.writeInt(m_size);
        p_exporter.writeByteArray(m_data);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_size = p_importer.readInt(m_size);
        m_data = p_importer.readByteArray(m_data);
    }

    @Override
    public int sizeofObject() {
        return Byte.BYTES + Integer.BYTES + ObjectSizeUtil.sizeofByteArray(m_data);
    }

    @Override
    public @NotNull Iterator<Short> iterator() {
        return new Itr();
    }

    private class Itr implements Iterator<Short> {
        private short m_cursor = 0;

        @Override
        public boolean hasNext() {
            short cursor = m_cursor;

            while (cursor < m_size) {
                if (isSet(cursor)) {
                    return true;
                }
                m_cursor++;
            }

            return false;
        }

        @Override
        public Short next() {
            while (m_cursor < m_size) {
                if (isSet(m_cursor)) {
                    return m_cursor;
                }
                m_cursor++;
            }

            throw new NoSuchElementException();
        }
    }
}