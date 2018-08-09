package de.hhu.bsinfo.dxram.boot.raft;

import de.hhu.bsinfo.dxraft.data.RaftData;

public class PeerData implements RaftData {

    private short id;
    private String m_ip;
    private int m_port;
    private char cmdLineRole;
    private short cmdLineRack;
    private short cmdLineSwitch;

    public PeerData(short p_id, String p_ip, int p_port, char p_cmdLineRole, short p_cmdLineRack, short p_cmdLineSwitch) {
        id = p_id;
        m_ip = p_ip;
        m_port = p_port;
        cmdLineRole = p_cmdLineRole;
        cmdLineRack = p_cmdLineRack;
        cmdLineSwitch = p_cmdLineSwitch;
    }

    public short getId() {
        return id;
    }

    public String getIp() {
        return m_ip;
    }

    public int getPort() {
        return m_port;
    }

    public char getCmdLineRole() {
        return cmdLineRole;
    }

    public short getCmdLineRack() {
        return cmdLineRack;
    }

    public short getCmdLineSwitch() {
        return cmdLineSwitch;
    }
}
