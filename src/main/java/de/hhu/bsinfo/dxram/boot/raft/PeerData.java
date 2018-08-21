package de.hhu.bsinfo.dxram.boot.raft;

import de.hhu.bsinfo.dxram.util.NodeCapabilities;

public class PeerData {
    private short id;
    private String m_ip;
    private int m_port;
    private char m_cmdLineRole;
    private short m_cmdLineRack;
    private short m_cmdLineSwitch;
    private int m_dxraftPort;
    private int m_capabilities;
    private int m_readFromFile;


    public PeerData(short p_id, String p_ip, int p_port, char p_cmdLineRole, short p_cmdLineRack, short p_cmdLineSwitch, int p_dxraftPort) {
        id = p_id;
        m_ip = p_ip;
        m_port = p_port;
        m_cmdLineRole = p_cmdLineRole;
        m_cmdLineRack = p_cmdLineRack;
        m_cmdLineSwitch = p_cmdLineSwitch;
        m_dxraftPort = p_dxraftPort;
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
        return m_cmdLineRole;
    }

    public short getCmdLineRack() {
        return m_cmdLineRack;
    }

    public short getCmdLineSwitch() {
        return m_cmdLineSwitch;
    }

    public int getDxraftPort() {
        return m_dxraftPort;
    }

}
