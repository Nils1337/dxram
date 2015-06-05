
package de.uniduesseldorf.dxram.core.log.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.log.LogHandler;
import de.uniduesseldorf.dxram.core.log.LogHandler.SecondaryLogsReorgThread;

/**
 * This class implements the secondary log
 * @author Kevin Beineke 23.10.2014
 */
public class SecondaryLogWithSegments extends AbstractLog implements LogStorageInterface {

	// TODO: Three secondary logs per node to accelerate recovery (recover
	// everything from primary)

	// Attributes
	private short m_nodeID;
	private int m_numberOfDeletes;
	private AtomicBoolean m_isLocked;

	private long m_secondaryLogReorgThreshold;
	private SecondaryLogsReorgThread m_reorganizationThread;

	private long m_totalUsableSpace;

	private SegmentHeader[] m_segmentHeaders;
	private LinkedList<Short> m_freeSegments;
	private LinkedList<Short> m_partlyUsedSegments;

	private boolean m_isAccessed;
	private SegmentHeader m_activeSegment;

	// Constructors
	/**
	 * Creates an instance of SecondaryLog with default configuration
	 * @param p_reorganizationThread
	 *            the reorganization thread
	 * @param p_nodeID
	 *            the NodeID
	 * @throws IOException
	 *             if secondary could not be created
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 */
	public SecondaryLogWithSegments(
			final SecondaryLogsReorgThread p_reorganizationThread,
			final short p_nodeID) throws IOException, InterruptedException {
		super(new File(LogHandler.BACKUP_DIRECTORY + "N"
				+ NodeID.getLocalNodeID() + "_"
				+ LogHandler.SECLOG_PREFIX_FILENAME + p_nodeID
				+ LogHandler.SECLOG_POSTFIX_FILENAME),
				LogHandler.SECONDARY_LOG_SIZE, LogHandler.SECLOG_HEADER_SIZE);

		m_nodeID = p_nodeID;
		m_numberOfDeletes = 0;

		m_totalUsableSpace = super.getTotalUsableSpace();

		m_reorganizationThread = p_reorganizationThread;

		m_secondaryLogReorgThreshold = (int) (LogHandler.SECONDARY_LOG_SIZE
				* (LogHandler.REORG_UTILIZATION_THRESHOLD / 100));

		m_isLocked = new AtomicBoolean(false);
		m_segmentHeaders = new SegmentHeader[(int) (LogHandler.SECONDARY_LOG_SIZE / LogHandler.SEGMENT_SIZE)];
		m_freeSegments = new LinkedList<Short>();
		for (int i = (int) (LogHandler.SECONDARY_LOG_SIZE / LogHandler.SEGMENT_SIZE - 1); i >= 0; i--) {
			m_freeSegments.add((short) i);
		}
		m_partlyUsedSegments = new LinkedList<Short>();

		createLogAndWriteHeader();
	}

	/**
	 * Creates an instance of SecondaryLog with default configuration except
	 * secondary log size
	 * @param p_secLogSize
	 *            the size of the secondary log
	 * @param p_reorganizationThread
	 *            the reorganization thread
	 * @param p_nodeID
	 *            the NodeID
	 * @throws IOException
	 *             if secondary log could not be created
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 */
	public SecondaryLogWithSegments(final long p_secLogSize,
			final SecondaryLogsReorgThread p_reorganizationThread,
			final short p_nodeID) throws IOException, InterruptedException {
		super(new File(LogHandler.BACKUP_DIRECTORY + "N"
				+ NodeID.getLocalNodeID() + "_"
				+ LogHandler.SECLOG_PREFIX_FILENAME + p_nodeID
				+ LogHandler.SECLOG_POSTFIX_FILENAME), p_secLogSize,
				LogHandler.SECLOG_HEADER_SIZE);
		if (p_secLogSize < LogHandler.SECONDARY_LOG_MIN_SIZE) {
			throw new IllegalArgumentException("Error: Secondary log too small");
		}

		m_nodeID = p_nodeID;
		m_numberOfDeletes = 0;

		m_totalUsableSpace = super.getTotalUsableSpace();

		m_reorganizationThread = p_reorganizationThread;

		m_secondaryLogReorgThreshold = (int) (p_secLogSize * (LogHandler.REORG_UTILIZATION_THRESHOLD / 100));

		m_isLocked = new AtomicBoolean(false);
		m_segmentHeaders = new SegmentHeader[(int) (LogHandler.SECONDARY_LOG_SIZE / LogHandler.SEGMENT_SIZE)];
		m_freeSegments = new LinkedList<Short>();
		for (int i = (int) (LogHandler.SECONDARY_LOG_SIZE / LogHandler.SEGMENT_SIZE - 1); i >= 0; i--) {
			m_freeSegments.add((short) i);
		}
		m_partlyUsedSegments = new LinkedList<Short>();

		createLogAndWriteHeader();
	}

	// Methods
	@Override
	public final void closeLog() throws InterruptedException, IOException {

		super.closeRing();
	}

	@Override
	public final int appendData(final byte[] p_data, final int p_offset,
			final int p_length, final Object p_unused) throws IOException,
			InterruptedException {
		int length = p_length;
		SegmentHeader header;
		short segment = -1;
		short firstSegment = -1;
		int offset;
		int logEntrySize;
		int rangeSize;

		if (length <= 0 || length > m_totalUsableSpace) {
			throw new IllegalArgumentException("Error: Invalid data size");
		} else {
			while (getWritableSpace() < length) {
				System.out.println("Secondary log for " + getNodeID()
						+ " is full. Initializing reorganization.");
				signalReorganizationAndWait();
			}

			/*
			 * Appending data cases:
			 * 1. This secondary log is accessed by the reorganization thread:
			 * a. No active segment or buffer too large to fit in: Create (new) "active segment" with given data
			 * b. Put data in currently active segment
			 * 2.
			 * a. Buffer is large (at least 90% of segment size): Create new segment and append it
			 * b. Fill partly used segments and put the rest (if there is data left) in a new segment and append it
			 */
			if (m_isAccessed) {
				// Reorganization thread is working on this secondary log -> only append data
				System.out.println("Writing on seclog that is currently reorganized");

				if (m_activeSegment == null || length + m_activeSegment.m_usedBytes > LogHandler.SEGMENT_SIZE) {
					// Create new segment and fill it
					segment = m_freeSegments.removeLast();
					header = new SegmentHeader(segment, length);
					m_segmentHeaders[segment] = header;
					writeToLog(p_data, p_offset, (long) segment
							* LogHandler.SEGMENT_SIZE, length, true);
					if (header.getFreeBytes() > LogHandler.SECONDARY_HEADER_SIZE) {
						m_partlyUsedSegments.add(segment);
					}
					m_activeSegment = header;
					System.out.println("Active segment: " + segment);
				} else {
					// Fill active segment
					writeToLog(p_data, p_offset, m_activeSegment.getIndex() * LogHandler.SEGMENT_SIZE
							+ m_activeSegment.getUsedBytes(), length, true);
					m_activeSegment.updateUsedBytes(length);
				}
			} else {
				if (length >= LogHandler.SEGMENT_SIZE * 0.9) {
					if (m_freeSegments.size() > 0) {
						// Create new segment and fill it
						segment = m_freeSegments.removeLast();
						header = new SegmentHeader(segment, length);
						m_segmentHeaders[segment] = header;
						offset = p_length - length + p_offset;
						writeToLog(p_data, offset, (long) segment
								* LogHandler.SEGMENT_SIZE, length, false);
						if (header.getFreeBytes() > LogHandler.SECONDARY_HEADER_SIZE) {
							m_partlyUsedSegments.add(segment);
						}
						length = 0;
					}
				}

				if (length > 0) {
					// Fill partly used segments if log iteration (remove task)
					// is not in progress
					if (m_partlyUsedSegments.size() > 0) {
						firstSegment = m_partlyUsedSegments.getFirst();
						while (m_partlyUsedSegments.size() > 0 && length > 0
								&& firstSegment != segment) {
							segment = m_partlyUsedSegments.removeLast();
							header = m_segmentHeaders[segment];
							offset = p_length - length + p_offset;
							rangeSize = 0;
							while (length - rangeSize > 0) {
								logEntrySize = LogHandler.SECONDARY_HEADER_SIZE
										+ getLengthOfLogEntry(p_data, offset
												+ rangeSize, false);
								if (header.getFreeBytes() - rangeSize > logEntrySize) {
									rangeSize += logEntrySize;
								} else {
									break;
								}
							}
							if (rangeSize > 0) {
								writeToLog(p_data, offset,
										(long) segment * LogHandler.SEGMENT_SIZE
												+ header.getUsedBytes(), rangeSize, false);
								header.updateUsedBytes(rangeSize);
								length -= rangeSize;
								offset += rangeSize;
							}
							if (header.getFreeBytes() > LogHandler.SECONDARY_HEADER_SIZE) {
								m_partlyUsedSegments.addFirst(segment);
							}
						}
					}
					if (length > 0) {
						// There are still objects in buffer -> create new
						// segment and fill it
						if (m_freeSegments.size() > 0) {
							segment = m_freeSegments.removeLast();
							header = new SegmentHeader(segment, length);
							m_segmentHeaders[segment] = header;
							offset = p_length - length + p_offset;
							writeToLog(p_data, offset, (long) segment
									* LogHandler.SEGMENT_SIZE, length, false);
							if (header.getFreeBytes() > LogHandler.SECONDARY_HEADER_SIZE) {
								m_partlyUsedSegments.add(segment);
							}
							length = 0;
						} else {
							System.out.println("Error: Secondary Log full!");
						}
					}
				}
			}
		}

		if (isReorganizationThresholdReached()) {
			signalReorganization();
		}

		return p_length - length;
	}

	/**
	 * Returns whether this secondary log is currently accessed by reorg. thread
	 * @return whether this secondary log is currently accessed by reorg. thread
	 */
	public final boolean isAccessed() {
		return m_isAccessed;
	}

	/**
	 * Sets the access flag
	 * @param p_flag
	 *            the new status
	 */
	public final void setAccessFlag(final boolean p_flag) {
		m_isAccessed = p_flag;
	}

	/**
	 * Returns the NodeID
	 * @return the NodeID
	 */
	public final short getNodeID() {
		return m_nodeID;
	}

	/**
	 * Returns the Segment Header
	 * @param p_index
	 *            the segment index
	 * @return the Segment Header
	 */
	public final SegmentHeader getSegmentHeader(final int p_index) {
		return m_segmentHeaders[p_index];
	}

	/**
	 * Returns the delete counter
	 * @return the delete counter
	 */
	public final int getDeleteCounter() {
		return m_numberOfDeletes;
	}

	/**
	 * Increments delete counter
	 */
	public final void incDeleteCounter() {
		m_numberOfDeletes++;
	}

	/**
	 * Locks the log
	 */
	public final void lock() {
		while (!m_isLocked.compareAndSet(false, true)) {
			try {
				Thread.sleep(100);
			} catch (final InterruptedException e) {}
		}
	}

	/**
	 * Unlocks the log
	 */
	public final void unlock() {
		m_isLocked.set(false);
	}

	/**
	 * Frees segment
	 * @param p_segment
	 *            the segment
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @note executed only by reorganization thread
	 */
	public final void freeSegment(final int p_segment) throws IOException,
			InterruptedException {
		short segment;
		SegmentHeader header;

		for (int i = 0; i < m_partlyUsedSegments.size(); i++) {
			segment = m_partlyUsedSegments.get((short) i);
			if (segment == p_segment) {
				m_partlyUsedSegments.remove((short) i);
				break;
			}
		}
		m_freeSegments.add((short) p_segment);

		header = m_segmentHeaders[p_segment];
		removeFromLog(header.getUsedBytes());
		header.reset();
	}

	/**
	 * Invalidates log entry
	 * @param p_buffer
	 *            the buffer
	 * @param p_bufferOffset
	 *            the buffer offset
	 * @param p_logOffset
	 *            the log offset
	 * @param p_segmentIndex
	 *            the segment index
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @note executed only by reorganization thread
	 */
	public final void invalidateLogEntry(final byte[] p_buffer,
			final int p_bufferOffset, final long p_logOffset,
			final int p_segmentIndex) throws IOException, InterruptedException {

		markLogEntryAsInvalid(p_buffer, p_bufferOffset);

		m_segmentHeaders[p_segmentIndex]
				.updateDeletedBytes(LogHandler.SECONDARY_HEADER_SIZE
						+ getLengthOfLogEntry(p_buffer, p_bufferOffset, false));
	}

	/**
	 * Updates log segment
	 * @param p_buffer
	 *            the buffer
	 * @param p_length
	 *            the segment length
	 * @param p_segmentIndex
	 *            the segment index
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @note executed only by reorganization thread
	 */
	public final void updateSegment(final byte[] p_buffer, final int p_length, final int p_segmentIndex)
			throws IOException, InterruptedException {

		overwriteLog(p_buffer, 0, p_segmentIndex * LogHandler.SEGMENT_SIZE,
				p_length, true);
	}

	/**
	 * Returns given segment of secondary log
	 * @param p_segment
	 *            the segment
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return the segment's data
	 * @note executed only by reorganization thread
	 */
	public final byte[] readSegment(final int p_segment) throws IOException,
			InterruptedException {
		byte[] result = null;
		SegmentHeader header;
		int length;

		header = m_segmentHeaders[p_segment];
		if (header != null) {
			length = header.getUsedBytes();
			result = new byte[length];
			readOnRAFRingRandomly(result, length, p_segment
					* LogHandler.SEGMENT_SIZE, true);
		}
		return result;
	}

	/**
	 * Returns all segments of secondary log
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return all data
	 * @note executed only by reorganization thread
	 */
	public final byte[][] readAllSegments() throws IOException,
			InterruptedException {
		byte[][] result = null;
		SegmentHeader header;
		int length;

		result = new byte[m_segmentHeaders.length][];
		for (int i = 0; i < m_segmentHeaders.length; i++) {
			header = m_segmentHeaders[i];
			if (header != null) {
				length = header.getUsedBytes();
				result[i] = new byte[length];
				readOnRAFRingRandomly(result[i], length, i
						* LogHandler.SEGMENT_SIZE, true);
			}
		}
		return result;
	}

	/**
	 * Returns all segments but the active one of secondary log
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return all non-active data
	 * @note executed only by reorganization thread
	 */
	public final byte[][] readAllNonActiveSegments() throws IOException,
			InterruptedException {
		byte[][] result = null;
		SegmentHeader header;
		int length;

		result = new byte[m_segmentHeaders.length][];
		for (int i = 0; i < m_segmentHeaders.length; i++) {
			if (m_activeSegment == null || i < m_activeSegment.getIndex()) {
				header = m_segmentHeaders[i];
				if (header != null) {
					length = header.getUsedBytes();
					result[i] = new byte[length];
					readOnRAFRingRandomly(result[i], length, i
							* LogHandler.SEGMENT_SIZE, true);
				}
			}
		}
		return result;
	}

	/**
	 * Returns all data of secondary log
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return all data
	 */
	public final byte[][] readAllNodeData() throws IOException,
			InterruptedException {
		byte[][] result = null;

		result = readAll(true);

		return result;
	}

	/**
	 * Returns a list with all log entries wrapped in chunks
	 * @param p_doCRCCheck
	 *            whether to check the payload or not
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return ArrayList with all log entries as chunks
	 */
	public final ArrayList<Chunk> recoverAllLogEntries(
			final boolean p_doCRCCheck) throws IOException,
			InterruptedException {
		int i = 0;
		int offset = 0;
		int logEntrySize;
		int payloadSize;
		long checksum;
		long chunkID;
		byte[][] logData;
		byte[] payload;
		HashMap<Long, Chunk> chunkMap = null;

		// TODO: Guarantee that there is no more data to come
		// TODO: Coordinate with reorganization thread
		try {
			logData = readAllWithoutReadPtrSet(false);
			while (logData[i] != null) {
				chunkMap = new HashMap<Long, Chunk>();
				while (offset + LogHandler.PRIMARY_HEADER_SIZE < logData[i].length) {
					// Determine header of next log entry
					chunkID = getChunkIDOfLogEntry(logData[i], offset);
					payloadSize = getLengthOfLogEntry(logData[i], offset, false);
					checksum = getChecksumOfPayload(logData[i], offset, false);
					logEntrySize = LogHandler.PRIMARY_HEADER_SIZE + payloadSize;

					if (logEntrySize > LogHandler.MIN_LOG_ENTRY_SIZE) {
						// Read payload and create chunk
						if (offset + logEntrySize <= logData[i].length) {
							// Create chunk only if log entry complete
							payload = new byte[payloadSize];
							System.arraycopy(logData[i], offset
									+ LogHandler.PRIMARY_HEADER_SIZE, payload,
									0, payloadSize);
							if (p_doCRCCheck) {
								if (calculateChecksumOfPayload(payload) != checksum) {
									// Ignore log entry
									offset += logEntrySize;
									continue;
								}
							}
							chunkMap.put(chunkID, new Chunk(chunkID, payload));
						}
					}
					offset += logEntrySize;
				}
				calcAndSetReadPos(offset);
				i++;
			}
		} finally {
			logData = null;
			payload = null;
		}
		return (ArrayList<Chunk>) chunkMap.values();
	}

	/**
	 * Returns a list with all log entries wrapped in chunks
	 * @param p_doCRCCheck
	 *            whether to check the payload or not
	 * @param p_low
	 *            lower bound
	 * @param p_high
	 *            higher bound
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return ArrayList with all log entries as chunks
	 */
	public final ArrayList<Chunk> recoverRange(final boolean p_doCRCCheck,
			final long p_low, final long p_high) throws IOException,
			InterruptedException {
		int i = 0;
		int offset = 0;
		int logEntrySize;
		int payloadSize;
		long checksum;
		long chunkID;
		long lid;
		byte[][] logData;
		byte[] payload;
		HashMap<Long, Chunk> chunkMap = null;

		// TODO: Guarantee that there is no more data to come
		// TODO: Coordinate with reorganization thread
		try {
			logData = readAllWithoutReadPtrSet(false);
			while (logData[i] != null) {
				chunkMap = new HashMap<Long, Chunk>();
				while (offset + LogHandler.PRIMARY_HEADER_SIZE < logData[i].length) {
					// Determine header of next log entry
					payloadSize = getLengthOfLogEntry(logData[i], offset, false);
					logEntrySize = LogHandler.PRIMARY_HEADER_SIZE + payloadSize;
					chunkID = getChunkIDOfLogEntry(logData[i], offset);
					lid = ChunkID.getLocalID(chunkID);
					if (lid >= p_low || lid <= p_high) {
						checksum = getChecksumOfPayload(logData[i], offset,
								false);

						if (logEntrySize > LogHandler.MIN_LOG_ENTRY_SIZE) {
							// Read payload and create chunk
							if (offset + logEntrySize <= logData[i].length) {
								// Create chunk only if log entry complete
								payload = new byte[payloadSize];
								System.arraycopy(logData[i], offset
										+ LogHandler.PRIMARY_HEADER_SIZE,
										payload, 0, payloadSize);
								if (p_doCRCCheck) {
									if (calculateChecksumOfPayload(payload) != checksum) {
										// Ignore log entry
										offset += logEntrySize;
										continue;
									}
								}
								chunkMap.put(chunkID, new Chunk(chunkID,
										payload));
							}
						}
					}
					offset += logEntrySize;
				}
				calcAndSetReadPos(offset);
				i++;
			}
		} finally {
			logData = null;
			payload = null;
		}
		return (ArrayList<Chunk>) chunkMap.values();
	}

	/**
	 * Checks if the threshold to reorganize is reached
	 * @return whether to reorganize or not
	 */
	public final boolean isReorganizationThresholdReached() {
		boolean ret = false;
		long bytesInRAF;

		bytesInRAF = getOccupiedSpace();
		if (bytesInRAF == 0) {
			ret = false;
		} else {
			if (bytesInRAF >= m_secondaryLogReorgThreshold) {
				ret = true;
			} else {
				ret = false;
			}
		}
		return ret;
	}

	/**
	 * Wakes up the reorganization thread
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	public final void signalReorganization() throws InterruptedException {

		try {
			m_reorganizationThread.lock();
			m_reorganizationThread.setLog(this);
			m_reorganizationThread.signal();
		} finally {
			m_reorganizationThread.unlock();
		}
	}

	/**
	 * Wakes up the reorganization thread and waits until reorganization is
	 * finished
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	public final void signalReorganizationAndWait() throws InterruptedException {

		try {
			m_reorganizationThread.lock();
			m_reorganizationThread.setLog(this);
			m_reorganizationThread.signal();
			m_reorganizationThread.await();
		} finally {
			m_reorganizationThread.unlock();
		}
	}

	/**
	 * Reorganizes all segments
	 */
	public final void reorganizeAll() {
		long timeStart;

		System.out.println();
		System.out.println("----------Starting complete reorganization(" + getNodeID()
				+ ")----------");
		timeStart = System.currentTimeMillis();

		for (int i = 0; i < m_segmentHeaders.length; i++) {
			if (m_segmentHeaders[i] != null) {
				reorganizeSegment(i);
			}
		}

		System.out.println("--Log processed in "
				+ (System.currentTimeMillis() - timeStart) + "ms");

		System.out.println("----------Finished reorganization(" + getNodeID()
				+ ")----------");
		System.out.println();
	}

	/**
	 * Reorganizes one segment by choosing the segment with best cost-benefit ratio
	 */
	public final void reorganizeIteratively() {
		int segmentIndex;
		long timeStart;

		System.out.println();
		System.out.println("----------Starting reorganization(" + getNodeID()
				+ ")----------");
		timeStart = System.currentTimeMillis();

		segmentIndex = chooseSegment();
		reorganizeSegment(segmentIndex);

		System.out.println("--Segment(" + segmentIndex + ") processed in "
				+ (System.currentTimeMillis() - timeStart) + "ms");
		System.out.println("----------Finished reorganization(" + getNodeID()
				+ ")----------");
		System.out.println();
	}

	/**
	 * Reorganizes one given segment
	 * @param p_segmentIndex
	 *            the segments index
	 */
	public final void reorganizeSegment(final int p_segmentIndex) {
		int length;
		int readBytes = 0;
		int writtenBytes = 0;
		int removedObjects = 0;
		int removedTombstones = 0;
		long localID;
		byte[] segmentData;
		byte[] newData;
		SegmentHeader header;

		System.out.println("--Segment: " + p_segmentIndex);
		if (-1 != p_segmentIndex) {
			try {
				segmentData = readSegment(p_segmentIndex);
				newData = new byte[LogHandler.SEGMENT_SIZE];

				// TODO: Remove all old versions in segment if a tombstone appears?
				// TODO: Remove object if there is a newer version in this segment?
				while (readBytes < segmentData.length) {
					length = LogHandler.SECONDARY_HEADER_SIZE
							+ getLengthOfLogEntry(segmentData, readBytes, false);
					localID = getLIDOfLogEntry(segmentData, readBytes, false);

					// Note: Out-dated and deleted objects' and tombstones' LIDs
					// are marked with -1 by remove task
					if (localID != (-1 & 0x0000FFFFFFFFFFFFL)) {
						System.arraycopy(segmentData, readBytes, newData,
								writtenBytes, length);
						writtenBytes += length;
					} else {
						if (length > LogHandler.SECONDARY_TOMBSTONE_SIZE) {
							removedObjects++;
						} else {
							removedTombstones++;
						}
					}
					readBytes += length;
				}
				if (writtenBytes < readBytes) {
					if (writtenBytes > 0) {
						newData = Arrays.copyOf(newData, writtenBytes);
						updateSegment(newData, newData.length, p_segmentIndex);
						header = getSegmentHeader(p_segmentIndex);
						header.reset();
						header.updateUsedBytes(writtenBytes);
					} else {
						freeSegment(p_segmentIndex);
						getSegmentHeader(p_segmentIndex).reset();
					}
				}
			} catch (final IOException | InterruptedException e) {
				System.out.println("Reorganization failed!");
			}

			System.out.println("--" + removedObjects + " entries removed");
			System.out
					.println("--" + removedTombstones + " tombstones removed");
		}
	}

	/**
	 * Determines the next segment to reorganize
	 * @return the chosen segment
	 */
	private int chooseSegment() {
		int ret = -1;
		long costBenefitRatio;
		long max = -1;
		SegmentHeader currentSegment;

		// Cost-benefit ratio: ((1-u)*age)/(1+u)
		for (int i = 0; i < m_segmentHeaders.length; i++) {
			currentSegment = m_segmentHeaders[i];
			if (currentSegment != null) {
				costBenefitRatio = (long) ((1 - currentSegment
						.getUtilization()) * currentSegment.getLastAccess() / (1 + currentSegment
						.getUtilization()));

				System.out.println("Cost-Benefit-Ratio: " + costBenefitRatio
						+ ", Age:" + currentSegment.getLastAccess()
						+ ", Utilization: " + currentSegment.getUtilization()
						+ ", Used Bytes: " + currentSegment.getUsedBytes());

				if (costBenefitRatio > max && (m_activeSegment == null || i != m_activeSegment.getIndex())) {
					max = costBenefitRatio;
					ret = i;
				}
			}
		}
		return ret;
	}

	/**
	 * Marks invalid (old or deleted) objects in complete log
	 * @param p_hashtable
	 *            a hash table to note version numbers in
	 */
	public final void markInvalidObjects(final VersionsHashTable p_hashtable) {
		long timeStart;
		int readBytes = 0;
		int hashVersion;
		int logVersion;
		long localID;
		boolean wasUpdated = false;
		byte[][] segments;
		byte[] segment;

		System.out.println();
		System.out.println("............Removing tombstones(" + getNodeID()
				+ ")............");
		timeStart = System.currentTimeMillis();

		p_hashtable.clear();
		try {
			segments = readAllNonActiveSegments();

			// Gather versions and tombstones
			for (int i = 0; i < segments.length; i++) {
				readBytes = 0;
				if (segments[i] != null) {
					segment = segments[i];
					System.out.println("Segment " + i + ": " + segment.length);
					while (readBytes < segment.length) {
						// Put object versions to hashtable
						// Collision: Store the higher version number if not
						// -1 (^= deleted)
						System.out.println(getVersionOfLogEntry(segment, readBytes, false));
						p_hashtable.putMax(getLIDOfLogEntry(segment, readBytes, false),
								getVersionOfLogEntry(segment, readBytes, false));
						readBytes += LogHandler.SECONDARY_HEADER_SIZE
								+ getLengthOfLogEntry(segment, readBytes, false);
					}
				}
			}
			readBytes = 0;
			System.out.println("..Entries in hashtable: " + p_hashtable.size());

			// Mark out-dated and deleted entries
			for (int i = 0; i < segments.length; i++) {
				readBytes = 0;
				if (segments[i] != null) {
					segment = segments[i];
					while (readBytes < segment.length) {
						localID = getLIDOfLogEntry(segment, readBytes, false);
						hashVersion = p_hashtable.get(localID);
						logVersion = getVersionOfLogEntry(segment, readBytes, false);

						if ((hashVersion == -1 || hashVersion > logVersion)
								&& localID != (-1 & 0x0000FFFFFFFFFFFFL)) {
							// Set LID of out-dated and deleted objects and
							// tombstones to -1
							System.out.println("##################Found deleted chunk version#################");
							invalidateLogEntry(segment, readBytes,
									i * LogHandler.SEGMENT_SIZE + readBytes, i);
							wasUpdated = true;
						}
						readBytes += LogHandler.SECONDARY_HEADER_SIZE
								+ getLengthOfLogEntry(segment, readBytes, false);
					}
					if (wasUpdated) {
						updateSegment(segment, readBytes, i);
						wasUpdated = false;
					}
				}
			}
		} catch (final IOException | InterruptedException e) {
			System.out.println("Removing tombstones failed!");
		}

		System.out.println("..All log entries processed in "
				+ (System.currentTimeMillis() - timeStart) + "ms");
		System.out.println(".............Finished removing(" + getNodeID()
				+ ").............");
		System.out.println();
	}

	/**
	 * Prints all segment sizes
	 */
	public final void printSegmentDistribution() {
		String s = "";
		int counter = 0;

		for (SegmentHeader header : m_segmentHeaders) {
			if (header != null) {
				if (counter == 18) {
					System.out.println(s);
					counter = 0;
					s = "";
				}
				s += header.getUsedBytes() + "\t";
				counter++;
			}
		}
		System.out.println(s);
		System.out.println();
		System.out.println();
	}

	// Classes
	/**
	 * SegmentHeader
	 * @author Kevin Beineke 07.11.2014
	 */
	public class SegmentHeader {

		// Attributes
		private int m_index;
		private int m_usedBytes;
		private int m_deletedBytes;
		private long m_lastAccess;

		// Constructors
		/**
		 * Creates an instance of SegmentHeader
		 * @param p_usedBytes
		 *            the number of used bytes
		 * @param p_index
		 *            the index within the log
		 */
		public SegmentHeader(final int p_index, final int p_usedBytes) {
			m_index = p_index;
			m_usedBytes = p_usedBytes;
			m_deletedBytes = 0;
			m_lastAccess = System.currentTimeMillis();
		}

		// Getter
		/**
		 * Returns the utilization
		 * @return the utilization
		 */
		public final float getUtilization() {
			float ret = 1;

			if (m_usedBytes > 0) {
				ret = (float) (m_usedBytes - m_deletedBytes)
						/ LogHandler.SEGMENT_SIZE;
			}
			return ret;
		}

		/**
		 * Returns the index
		 * @return the index
		 */
		public final int getIndex() {
			return m_index;
		}

		/**
		 * Returns number of used bytes
		 * @return number of used bytes
		 */
		public final int getUsedBytes() {
			return m_usedBytes;
		}

		/**
		 * Returns number of deleted bytes
		 * @return number of deleted bytes
		 */
		public final int getDeletedBytes() {
			return m_deletedBytes;
		}

		/**
		 * Returns number of used bytes
		 * @return number of used bytes
		 */
		public final int getFreeBytes() {
			return LogHandler.SEGMENT_SIZE - m_usedBytes;
		}

		/**
		 * Returns the timestamp of last access
		 * @return the timestamp of last access
		 */
		public final long getLastAccess() {
			return m_lastAccess;
		}

		// Setter
		/**
		 * Updates the number of used bytes
		 * @param p_writtenBytes
		 *            the number of written bytes
		 */
		public final void updateUsedBytes(final int p_writtenBytes) {
			m_usedBytes += p_writtenBytes;
			m_lastAccess = System.currentTimeMillis();
		}

		/**
		 * Updates the number of deleted bytes
		 * @param p_deletedBytes
		 *            the number of deleted bytes
		 */
		public final void updateDeletedBytes(final int p_deletedBytes) {
			m_deletedBytes += p_deletedBytes;
			m_lastAccess = System.currentTimeMillis();
		}

		/**
		 * Resets the segment header
		 */
		public final void reset() {
			m_deletedBytes = 0;
			m_usedBytes = 0;
			m_lastAccess = System.currentTimeMillis();
		}
	}

}
