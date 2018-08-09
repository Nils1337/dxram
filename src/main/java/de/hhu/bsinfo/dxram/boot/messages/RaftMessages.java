package de.hhu.bsinfo.dxram.boot.messages;

public final class RaftMessages {
    public static final byte SUBTYPE_SERVER_MESSAGE_WRAPPER = 1;
    public static final byte SUBTYPE_REQUEST_WRAPPER = 2;
    public static final byte SUBTYPE_CLIENT_RESPONSE_WRAPPER = 3;

    /**
     * Static class
     */
    private RaftMessages() {
    }
}
