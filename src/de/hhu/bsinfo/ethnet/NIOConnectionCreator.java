/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.ethnet;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Creates and manages new network connections using Java NIO
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 *         Marc Ewert, marc.ewert@hhu.de, 11.08.2014
 */
class NIOConnectionCreator extends AbstractConnectionCreator {

    private static final Logger LOGGER = LogManager.getFormatterLogger(NIOConnectionCreator.class.getSimpleName());

    // Attributes
    private MessageCreator m_messageCreator;
    private NIOSelector m_nioSelector;

    private MessageDirectory m_messageDirectory;
    private NIOInterface m_nioInterface;
    private NodeMap m_nodeMap;

    private short m_ownNodeID;

    private int m_incomingBufferSize;
    private int m_outgoingBufferSize;
    private int m_numberOfBuffersPerConnection;
    private int m_flowControlWindowSize;
    private int m_connectionTimeout;

    // Constructors

    /**
     * Creates an instance of NIOConnectionCreator
     *
     * @param p_messageDirectory
     *     the message directory
     * @param p_nodeMap
     *     the node map
     * @param p_ownNodeID
     *     the NodeID of this node
     * @param p_incomingBufferSize
     *     the size of incoming buffer
     * @param p_outgoingBufferSize
     *     the size of outgoing buffer
     * @param p_numberOfBuffersPerConnection
     *     the number of bytes until a flow control message must be received to continue sending
     * @param p_flowControlWindowSize
     *     the maximal number of ByteBuffer to schedule for sending/receiving
     * @param p_connectionTimeout
     *     the connection timeout
     */
    NIOConnectionCreator(final MessageDirectory p_messageDirectory, final NodeMap p_nodeMap, final short p_ownNodeID, final int p_incomingBufferSize,
        final int p_outgoingBufferSize, final int p_numberOfBuffersPerConnection, final int p_flowControlWindowSize, final int p_connectionTimeout) {
        super();

        m_nioSelector = null;

        m_messageDirectory = p_messageDirectory;

        m_incomingBufferSize = p_incomingBufferSize;
        m_outgoingBufferSize = p_outgoingBufferSize;
        m_numberOfBuffersPerConnection = p_numberOfBuffersPerConnection;
        m_flowControlWindowSize = p_flowControlWindowSize;
        m_connectionTimeout = p_connectionTimeout;

        m_nodeMap = p_nodeMap;
        m_ownNodeID = p_ownNodeID;

        m_nioInterface = new NIOInterface(p_incomingBufferSize, p_outgoingBufferSize);
    }

    // Methods

    @Override
    public String getSelectorStatus() {
        return m_nioSelector.toString();
    }

    /**
     * Initializes the creator
     */
    @Override
    public void initialize(final short p_nodeID, final int p_listenPort) {
        // #if LOGGER >= INFO
        LOGGER.info("Network: MessageCreator");
        // #endif /* LOGGER >= INFO */
        m_messageCreator = new MessageCreator(m_numberOfBuffersPerConnection);
        m_messageCreator.setName("Network: MessageCreator");
        m_messageCreator.start();

        // #if LOGGER >= INFO
        LOGGER.info("Network: NIOSelector");
        // #endif /* LOGGER >= INFO */
        m_nioSelector = new NIOSelector(this, m_nioInterface, p_listenPort, m_connectionTimeout);
        m_nioSelector.setName("Network: NIOSelector");
        m_nioSelector.start();
    }

    /**
     * Closes the creator and frees unused resources
     */
    @Override
    public void close() {
        m_nioSelector.close();
        m_nioSelector = null;

        m_messageCreator.shutdown();
        m_messageCreator = null;
    }

    @Override
    public boolean keyIsPending() {
        byte counter = 0;

        try {
            Iterator<SelectionKey> iter = m_nioSelector.getSelector().keys().iterator();
            while (iter.hasNext()) {
                if (iter.next().attachment() == null && ++counter == 2) {
                    return true;
                }
            }
        } catch (final ConcurrentModificationException e) {
            // A connection was closed during iteration -> try again
            return keyIsPending();
        }

        return false;
    }

    /**
     * Creates a new connection to the given destination
     *
     * @param p_destination
     *     the destination
     * @return a new connection
     * @throws IOException
     *     if the connection could not be created
     */
    @Override
    public NIOConnection createConnection(final short p_destination) throws IOException {
        NIOConnection ret;
        ReentrantLock condLock;
        Condition cond;
        long timeStart;
        long timeNow;

        condLock = new ReentrantLock(false);
        cond = condLock.newCondition();
        ret = new NIOConnection(p_destination, m_nodeMap, m_messageDirectory, condLock, cond, m_messageCreator, m_nioSelector, m_numberOfBuffersPerConnection,
            m_incomingBufferSize, m_outgoingBufferSize, m_flowControlWindowSize);

        ret.connect();

        timeStart = System.currentTimeMillis();
        condLock.lock();
        while (!ret.isConnected()) {
            timeNow = System.currentTimeMillis();
            if (timeNow - timeStart > m_connectionTimeout) {
                // #if LOGGER >= DEBUG
                LOGGER.debug("connection creation time-out. Interval %d ms might be to small", m_connectionTimeout);
                // #endif /* LOGGER >= DEBUG */

                condLock.unlock();
                throw new IOException("Timeout occurred");
            }
            try {
                cond.awaitNanos(1000);
            } catch (final InterruptedException e) { /* ignore */ }
        }
        condLock.unlock();

        // Close new connection if this node's NodeID is smaller. Own NodeID was transmitted triggering remote node to
        // establish a connection.
        if (m_ownNodeID < p_destination) {

            ret.closeGracefully();
            ret.cleanup();

            return null;
        }

        return ret;
    }

    @Override
    public void closeConnection(final AbstractConnection p_connection, final boolean p_informConnectionManager) {
        SelectionKey key;

        if (p_connection instanceof NIOConnection) {
            NIOConnection connection = (NIOConnection) p_connection;

            key = connection.getChannel().keyFor(m_nioSelector.getSelector());
            if (key != null) {
                key.cancel();

                try {
                    connection.getChannel().close();
                } catch (final IOException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Could not close connection to %s!", p_connection.getDestination());
                    // #endif /* LOGGER >= ERROR */
                }
                if (p_informConnectionManager) {
                    fireConnectionClosed(p_connection);
                }
            }
        }
    }

    /**
     * Creates a new connection, triggered by incoming key
     * m_buffer needs to be synchronized externally
     *
     * @param p_channel
     *     the channel of the connection
     * @throws IOException
     *     if the connection could not be created
     */
    void createConnection(final SocketChannel p_channel) throws IOException {
        NIOConnection connection;
        short remoteNodeID;

        try {
            remoteNodeID = m_nioInterface.readRemoteNodeID(p_channel, m_nioSelector);
            if (remoteNodeID != -1) {
                if (remoteNodeID > m_ownNodeID) {
                    // Remote node is allowed to open this connection -> proceed
                    connection = new NIOConnection(remoteNodeID, m_nodeMap, m_messageDirectory, p_channel, m_messageCreator, m_nioSelector,
                        m_numberOfBuffersPerConnection, m_incomingBufferSize, m_outgoingBufferSize, m_flowControlWindowSize);
                    // Register connection as attachment
                    p_channel.register(m_nioSelector.getSelector(), SelectionKey.OP_READ, connection);

                    fireConnectionCreated(connection);
                } else {
                    p_channel.keyFor(m_nioSelector.getSelector()).cancel();
                    p_channel.close();

                    fireCreateConnection(remoteNodeID);
                }
            } else {
                throw new IOException();
            }
        } catch (final IOException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not create connection!");
            // #endif /* LOGGER >= ERROR */
            throw e;
        }
    }

}
