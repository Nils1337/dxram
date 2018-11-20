package de.hhu.bsinfo.dxram.boot;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxraft.client.ClientConfig;
import de.hhu.bsinfo.dxraft.server.ServerConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

@Data
@Accessors(prefix = "m_")
public class DXRaftNodeRegistryConfig {

    @Expose
    private boolean m_bootstrapPeer = true;

    @Expose
    private ServerConfig m_raftServerConfig = new ServerConfig();

    @Expose
    private ClientConfig m_raftClientConfig = new ClientConfig();
}
