
package de.hhu.bsinfo.ethnet;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import de.hhu.bsinfo.dxram.net.events.ResponseDelayedEvent;

/**
 * Represents a Request
 * @author Florian Klein 09.03.2012
 */
public abstract class AbstractRequest extends AbstractMessage {

	// Constants
	private static final long WAITING_TIMEOUT = 20;

	// Attributes
	private boolean m_fulfilled;

	private boolean m_aborted;

	private boolean m_ignoreTimeout;

	private Semaphore m_wait;

	private AbstractResponse m_response;

	// Constructors
	/**
	 * Creates an instance of Request
	 */
	protected AbstractRequest() {
		super();

		m_wait = new Semaphore(0, false);

		m_response = null;
	}

	/**
	 * Creates an instance of Request
	 * @param p_destination
	 *            the destination
	 * @param p_type
	 *            the message type
	 * @param p_subtype
	 *            the message subtype
	 */
	public AbstractRequest(final short p_destination, final byte p_type, final byte p_subtype) {
		this(p_destination, p_type, p_subtype, DEFAULT_EXCLUSIVITY_VALUE);
	}

	/**
	 * Creates an instance of Request
	 * @param p_destination
	 *            the destination
	 * @param p_type
	 *            the message type
	 * @param p_subtype
	 *            the message subtype
	 * @param p_exclusivity
	 *            whether this request type allows parallel execution
	 */
	public AbstractRequest(final short p_destination, final byte p_type, final byte p_subtype, final boolean p_exclusivity) {
		super(p_destination, p_type, p_subtype, p_exclusivity);

		m_wait = new Semaphore(0, false);

		m_response = null;
	}

	// Getters
	/**
	 * Checks if the network timeout for the request should be ignored
	 * @return true if the timeout should be ignored, false otherwise
	 */
	public final boolean isIgnoreTimeout() {
		return m_ignoreTimeout;
	}

	/**
	 * Get the requestID
	 * @return the requestID
	 */
	public final int getRequestID() {
		return getMessageID();
	}

	/**
	 * Get the Response
	 * @return the Response
	 */
	public final AbstractResponse getResponse() {
		return m_response;
	}

	/**
	 * Get the Response
	 * @param <T>
	 *            the Response type
	 * @param p_class
	 *            the Class of the Response
	 * @return the Response
	 */
	public final <T extends AbstractResponse> T getResponse(final Class<T> p_class) {
		T ret = null;

		assert p_class != null;

		if (m_response != null && p_class.isAssignableFrom(m_response.getClass())) {
			ret = p_class.cast(m_response);
			m_response.setCorrespondingRequest(this);
		}

		return ret;
	}

	/**
	 * Checks if the Request is fulfilled
	 * @return true if the Request is fulfilled, false otherwise
	 */
	public final boolean isFulfilled() {
		return m_fulfilled;
	}

	/**
	 * Checks if the Request is aborted
	 * @return true if the Request is aborted, false otherwise
	 */
	public final boolean isAborted() {
		return m_aborted;
	}

	// Setters
	/**
	 * Set the ignore timeout option
	 * @param p_ignoreTimeout
	 *            if true the request ignores the network timeout
	 */
	public final void setIgnoreTimeout(final boolean p_ignoreTimeout) {
		m_ignoreTimeout = p_ignoreTimeout;
	}

	// Methods
	/**
	 * Wait until the Request is fulfilled or aborted
	 * @param p_timeoutMs
	 *            Max amount of time to wait for response.
	 * @return False if message timed out, true if response received.
	 */
	public final boolean waitForResponses(final int p_timeoutMs) {
		long timeStart;
		long timeNow;

		timeStart = System.currentTimeMillis();

		while (!m_fulfilled && !m_aborted) {
			timeNow = System.currentTimeMillis();
			if (timeNow - timeStart > p_timeoutMs && !m_ignoreTimeout) {
				// RequestStatistic.getInstance().requestTimeout(getRequestID(), getClass());
				break;
			}
			try {
				m_wait.tryAcquire(WAITING_TIMEOUT, TimeUnit.MILLISECONDS);
			} catch (final InterruptedException e) {}
		}

		if (!m_fulfilled && !m_aborted) {
			NetworkHandler.getEventHandler().fireEvent(new ResponseDelayedEvent(getClass().getSimpleName(), super.getDestination()));
		}

		return m_fulfilled;
	}

	/**
	 * Fulfill the Request
	 * @param p_response
	 *            the Response
	 */
	final void fulfill(final AbstractResponse p_response) {
		assert p_response != null;

		// RequestStatistic.getInstance().responseReceived(getRequestID(), getClass());

		m_response = p_response;

		m_fulfilled = true;
		m_wait.release();
	}

	@Override
	protected final void beforeSend() {
		RequestMap.put(this);
	}

	@Override
	protected final void afterSend() {
		// RequestStatistic.getInstance().requestSend(getRequestID());
	}

}