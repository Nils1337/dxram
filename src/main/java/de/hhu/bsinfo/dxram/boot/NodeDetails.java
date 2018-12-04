package de.hhu.bsinfo.dxram.boot;

import de.hhu.bsinfo.dxraft.data.DataTypes;
import de.hhu.bsinfo.dxraft.data.RaftData;
import de.hhu.bsinfo.dxram.boot.raft.Bitmap;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonRootName;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Objects;

@SuppressWarnings("unused")
@JsonRootName("node")
@Accessors(prefix = "m_")
public final class NodeDetails implements RaftData {

    // TODO probably better to make this registration non-static
    private static final byte NODE_DETAILS_TYPE = 51;

    static {
        DataTypes.registerDataType(NODE_DETAILS_TYPE, NodeDetails.class);
    }

    private byte[] m_data;
    /**
     * The node's id.
     */
    @Setter
    private short m_id;

    /**
     * The node's ip address.
     */
    private String m_ip;

    /**
     * The node's port.
     */
    private int m_port;

    /**
     * The node's rack id.
     */
    private short m_rack;

    /**
     * The node's switch id.
     */
    private short m_switch;

    /**
     * The node's role.
     */
    private NodeRole m_role;

    /**
     * Indicates whether this node is currently available.
     */
    private boolean m_online;

    /**
     * Indicates whether this node is able to store backups.
     */
    private boolean m_availableForBackup;

    /**
     * The node's capabilities.
     */
    private int m_capabilities;

    @JsonCreator
    private NodeDetails(
            @JsonProperty("id")
                    short p_id,
            @JsonProperty("ip")
                    String p_ip,
            @JsonProperty("port")
                    int p_port,
            @JsonProperty("rack")
                    short p_rack,
            @JsonProperty("switch")
                    short p_switch,
            @JsonProperty("role")
                    NodeRole p_role,
            @JsonProperty("online")
                    boolean p_online,
            @JsonProperty("availableForBackup")
                    boolean p_availableForBackup,
            @JsonProperty("capabilities")
                    int p_capabilities) {
        m_id = p_id;
        m_ip = p_ip;
        m_port = p_port;
        m_rack = p_rack;
        m_switch = p_switch;
        m_role = p_role;
        m_online = p_online;
        m_availableForBackup = p_availableForBackup;
        m_capabilities = p_capabilities;
    }

    public static Builder builder(final short p_id, final String p_ip, final int p_port) {
        return new Builder(p_id, p_ip, p_port);
    }

    public short getId() {
        return m_id;
    }

    public String getIp() {
        return m_ip;
    }

    public int getPort() {
        return m_port;
    }

    public short getRack() {
        return m_rack;
    }

    public short getSwitch() {
        return m_switch;
    }

    public NodeRole getRole() {
        return m_role;
    }

    public boolean isOnline() {
        return m_online;
    }

    public boolean isAvailableForBackup() {
        return m_availableForBackup;
    }

    public int getCapabilities() {
        return m_capabilities;
    }

    public InetSocketAddress getAddress() {
        return new InetSocketAddress(m_ip, m_port);
    }

    public NodeDetails withCapabilities(int p_capabilities) {
        return new NodeDetails(m_id, m_ip, m_port, m_rack, m_switch, m_role, m_online, m_availableForBackup,
                p_capabilities);
    }

    public NodeDetails withOnline(boolean p_isOnline) {
        return new NodeDetails(m_id, m_ip, m_port, m_rack, m_switch, m_role, p_isOnline, m_availableForBackup,
                m_capabilities);
    }

    @Override
    public boolean equals(Object p_o) {
        if (this == p_o) {
            return true;
        }
        if (p_o == null || getClass() != p_o.getClass()) {
            return false;
        }
        NodeDetails details = (NodeDetails) p_o;
        return m_id == details.m_id &&
                m_port == details.m_port &&
                m_rack == details.m_rack &&
                m_switch == details.m_switch &&
                m_online == details.m_online &&
                m_availableForBackup == details.m_availableForBackup &&
                m_capabilities == details.m_capabilities &&
                Objects.equals(m_ip, details.m_ip) &&
                m_role == details.m_role;
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_id);
    }

    @Override
    public String toString() {
        return String.format("[%c|%04X](%s:%d)", m_role.getAcronym(), m_id, m_ip, m_port);
    }

    ServiceInstance<NodeDetails> toServiceInstance(String p_serviceName) {
        return new ServiceInstance<>(p_serviceName, NodeID.toHexStringShort(m_id), m_ip,
                m_port, 0, this, System.currentTimeMillis(), ServiceType.DYNAMIC, null, true);
    }

    public byte[] toByteArray() {
        ByteBuffer buffer = ByteBuffer.allocate(3 * Short.BYTES + 2 * Integer.BYTES +
                Character.BYTES + 2 * Byte.BYTES + ObjectSizeUtil.sizeofString(m_ip));
        ByteBufferImExporter exporter = new ByteBufferImExporter(buffer);

        exporter.writeShort(m_id);
        exporter.writeString(m_ip);
        exporter.writeInt(m_port);
        exporter.writeShort(m_rack);
        exporter.writeShort(m_switch);
        exporter.writeChar(m_role.getAcronym());
        exporter.writeBoolean(m_online);
        exporter.writeBoolean(m_availableForBackup);
        exporter.writeInt(m_capabilities);

        return buffer.array();
    }

    public static NodeDetails fromByteArray(final byte[] p_bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(p_bytes);
        ByteBufferImExporter importer = new ByteBufferImExporter(buffer);

        short tmpId = 0;
        String tmpIp = "";
        int tmpPort = 0;
        short tmpRack = 0;
        short tmpSwitch = 0;
        char tmpRole = '0';
        boolean tmpOnline;
        boolean tmpBackup;
        int tmpCapabilites = 0;

        tmpId = importer.readShort(tmpId);
        tmpIp = importer.readString(tmpIp);
        tmpPort = importer.readInt(tmpPort);
        tmpRack = importer.readShort(tmpRack);
        tmpSwitch = importer.readShort(tmpSwitch);
        tmpRole = importer.readChar(tmpRole);
        tmpOnline = importer.readBoolean(false);
        tmpBackup = importer.readBoolean(false);
        tmpCapabilites = importer.readInt(tmpCapabilites);

        return new NodeDetails(tmpId, tmpIp, tmpPort, tmpRack, tmpSwitch,
                NodeRole.getRoleByAcronym(tmpRole), tmpOnline, tmpBackup, tmpCapabilites);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeShort(m_id);
        p_exporter.writeString(m_ip);
        p_exporter.writeInt(m_port);
        p_exporter.writeShort(m_rack);
        p_exporter.writeShort(m_switch);
        p_exporter.writeChar(m_role.getAcronym());
        p_exporter.writeBoolean(m_online);
        p_exporter.writeBoolean(m_availableForBackup);
        p_exporter.writeInt(m_capabilities);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_id = p_importer.readShort(m_id);
        m_ip = p_importer.readString(m_ip);
        m_port = p_importer.readInt(m_port);
        m_rack = p_importer.readShort(m_rack);
        m_switch = p_importer.readShort(m_switch);
        char c = p_importer.readChar('0');
        m_role = NodeRole.getRoleByAcronym(c);
        m_online = p_importer.readBoolean(m_online);
        m_availableForBackup = p_importer.readBoolean(m_availableForBackup);
        m_capabilities = p_importer.readInt(m_capabilities);
    }

    @Override
    public int sizeofObject() {
        return 3 * Short.BYTES + 2 * Integer.BYTES + ObjectSizeUtil.sizeofString(m_ip) + Character.BYTES +
                2 * ObjectSizeUtil.sizeofBoolean();
    }

    public static class Builder {

        private final short m_id;
        private final String m_ip;
        private final int m_port;

        private short m_rack;
        private short m_switch;
        private NodeRole m_role;
        private boolean m_online;
        private boolean m_availableForBackup;
        private int m_capabilities;

        public Builder(short p_id, String p_ip, int p_port) {
            m_id = p_id;
            m_ip = p_ip;
            m_port = p_port;
        }

        public Builder withRack(short p_rack) {
            m_rack = p_rack;
            return this;
        }

        public Builder withSwitch(short p_switch) {
            m_switch = p_switch;
            return this;
        }

        public Builder withRole(NodeRole p_role) {
            m_role = p_role;
            return this;
        }

        public Builder withOnline(boolean p_online) {
            m_online = p_online;
            return this;
        }

        public Builder withAvailableForBackup(boolean p_availableForBackup) {
            m_availableForBackup = p_availableForBackup;
            return this;
        }

        public Builder withCapabilities(int p_capabilities) {
            m_capabilities = p_capabilities;
            return this;
        }

        public NodeDetails build() {
            return new NodeDetails(m_id, m_ip, m_port, m_rack, m_switch, m_role, m_online,
                    m_availableForBackup, m_capabilities);
        }
    }
}
