package de.hhu.bsinfo.dxram.boot.raft;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxraft.context.RaftAddress;
import de.hhu.bsinfo.dxraft.message.client.ClientRequest;
import de.hhu.bsinfo.dxraft.message.RaftMessage;
import de.hhu.bsinfo.dxraft.message.server.ServerMessage;
import de.hhu.bsinfo.dxraft.net.AbstractNetworkService;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.messages.RaftClientResponseWrapper;
import de.hhu.bsinfo.dxram.boot.messages.RaftRequestWrapper;
import de.hhu.bsinfo.dxram.boot.messages.RaftServerMessageWrapper;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

import static de.hhu.bsinfo.dxram.boot.messages.RaftMessages.SUBTYPE_CLIENT_RESPONSE_WRAPPER;
import static de.hhu.bsinfo.dxram.boot.messages.RaftMessages.SUBTYPE_REQUEST_WRAPPER;
import static de.hhu.bsinfo.dxram.boot.messages.RaftMessages.SUBTYPE_SERVER_MESSAGE_WRAPPER;

public class DXRaftNetworkService extends AbstractNetworkService implements MessageReceiver {

    private final Logger LOGGER;

    private NetworkComponent m_networkComponent;

    public DXRaftNetworkService(NetworkComponent p_networkComponent) {
        LOGGER = LogManager.getFormatterLogger(getClass().getSimpleName());
        m_networkComponent = p_networkComponent;
        m_networkComponent.registerMessageType(DXRAMMessageTypes.RAFT_MESSAGES_TYPE, SUBTYPE_SERVER_MESSAGE_WRAPPER, RaftServerMessageWrapper.class);
        m_networkComponent.registerMessageType(DXRAMMessageTypes.RAFT_MESSAGES_TYPE, SUBTYPE_REQUEST_WRAPPER, RaftRequestWrapper.class);
        m_networkComponent.registerMessageType(DXRAMMessageTypes.RAFT_MESSAGES_TYPE, SUBTYPE_CLIENT_RESPONSE_WRAPPER, RaftClientResponseWrapper.class);
    }

    @Override
    public void sendMessage(RaftMessage p_message) {
        RaftServerMessageWrapper message = wrapMessage(p_message);
        if (message != null) {
            try {
                m_networkComponent.sendMessage(message);
            } catch (NetworkException e) {}
        }
    }

    @Override
    public RaftMessage sendRequest(ClientRequest p_request) {
        RaftRequestWrapper message = wrapRequest(p_request);
        if (message != null) {
            try {
                m_networkComponent.sendSync(message);
                RaftClientResponseWrapper response = message.getResponse(RaftClientResponseWrapper.class);
                return unwrapResponse(response);
            } catch (NetworkException e) {}
        }

        return null;
    }

    @Override
    public void startReceiving() {
        m_networkComponent.register(DXRAMMessageTypes.RAFT_MESSAGES_TYPE, SUBTYPE_SERVER_MESSAGE_WRAPPER, this);
    }

    @Override
    public void stopReceiving() {
        m_networkComponent.unregister(DXRAMMessageTypes.RAFT_MESSAGES_TYPE, SUBTYPE_SERVER_MESSAGE_WRAPPER, this);
    }

    @Override
    public void close() {
        // Nothing to do here
    }

    private RaftServerMessageWrapper wrapMessage(RaftMessage p_message) {
        try (
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ObjectOutputStream objOut = new ObjectOutputStream(out)
        )
        {
            objOut.writeObject(p_message);
            byte[] msg = out.toByteArray();

            RaftAddress receiverAddress = p_message.getReceiverAddress();

            return new RaftServerMessageWrapper(receiverAddress.getId().getValue(), DXRAMMessageTypes.RAFT_MESSAGES_TYPE, SUBTYPE_SERVER_MESSAGE_WRAPPER, msg);
        } catch (IOException e) {
            LOGGER.error("Exception wrapping raft message", e);
        }
        return null;
    }

    private RaftRequestWrapper wrapRequest(RaftMessage p_message) {
        try (
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ObjectOutputStream objOut = new ObjectOutputStream(out)
        )
        {
            objOut.writeObject(p_message);
            byte[] msg = out.toByteArray();

            RaftAddress receiverAddress = p_message.getReceiverAddress();

            return new RaftRequestWrapper(receiverAddress.getId().getValue(), DXRAMMessageTypes.RAFT_MESSAGES_TYPE, SUBTYPE_REQUEST_WRAPPER, msg);
        } catch (IOException e) {
            LOGGER.error("Exception wrapping raft message", e);
        }
        return null;
    }

    private RaftMessage unwrapMessage(RaftServerMessageWrapper p_wrapper) {
        ObjectInputStream objIn;
        try {
            objIn = new ObjectInputStream(new ByteArrayInputStream(p_wrapper.getMsgData()));
            return (RaftMessage) objIn.readObject();
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error("Exception unwrapping raft message", e);
        }
        return null;
    }

    private RaftMessage unwrapResponse(RaftClientResponseWrapper p_wrapper) {
        ObjectInputStream objIn;
        try {
            objIn = new ObjectInputStream(new ByteArrayInputStream(p_wrapper.getMsgData()));
            return (RaftMessage) objIn.readObject();
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error("Exception unwrapping raft message", e);
        }
        return null;
    }


    @Override
    public void onIncomingMessage(Message p_message) {
        if (p_message.getSubtype() == SUBTYPE_SERVER_MESSAGE_WRAPPER) {
            RaftMessage msg = unwrapMessage((RaftServerMessageWrapper) p_message);

            if (msg instanceof ServerMessage) {
                ((ServerMessage) msg).deliverMessage(getMessageReceiver());
                return;
            }
        }

        // should not happen
        LOGGER.error("Incoming message was no raft server message");
    }
}
