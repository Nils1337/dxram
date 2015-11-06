package de.uniduesseldorf.dxram.core.chunk.storage;

import java.util.Arrays;

import de.uniduesseldorf.dxram.core.exceptions.MemoryException;

/**
 * Defragments the memory periodical
 * @author Florian Klein
 *         05.04.2014
 */
public final class Defragmenter implements Runnable {

	// Constants
	private static final long SLEEP_TIME = 10000;
	private static final double MAX_FRAGMENTATION = 0.75;

	// Attributes
	private boolean m_running;

	// Constructors
	/**
	 * Creates an instance of Defragmenter
	 */
	private Defragmenter() {
		m_running = false;
	}

	// Methods
	/**
	 * Stops the defragmenter
	 */
	private void stop() {
		m_running = false;
	}

	@Override
	public void run() {
		long table;
		int offset;
		double[] fragmentation;

		offset = 0;
		m_running = true;
		while (m_running) {
			try {
				Thread.sleep(SLEEP_TIME);
			} catch (final InterruptedException e) {}

			if (m_running) {
				try {
					fragmentation = m_rawMemory.getFragmentation();

					table = getEntry(offset++, m_nodeIDTableDirectory, 1);
					if (table == 0) {
						offset = 0;
						table = getEntry(offset++, m_nodeIDTableDirectory, 1);
					}

					defragmentTable(table, 1, fragmentation);
				} catch (final MemoryException e) {}
			}
		}
	}

	/**
	 * Defragments a table and its subtables
	 * @param p_addressTable
	 *            the table to defragment
	 * @param p_level
	 *            the level of the table
	 * @param p_fragmentation
	 *            the current fragmentation
	 */
	private void defragmentTable(final long p_addressTable, final int p_level, final double[] p_fragmentation) {
		long entry;
		long address;
		long newAddress;
		int segment;
		byte[] data;

//		writeLock(p_addressTable);
		for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
			try {
				entry = readEntry(p_addressTable, i);
				address = entry & BITMASK_ADDRESS;
				newAddress = 0;

				if (address != 0) {
					segment = m_rawMemory.getSegment(address);

					if (p_level > 1) {
						defragmentTable(address, p_level - 1, p_fragmentation);

						if (p_fragmentation[segment] > MAX_FRAGMENTATION) {
							data = m_rawMemory.readBytes(address);
							m_rawMemory.free(address);
							newAddress = m_rawMemory.malloc(data.length);
							m_rawMemory.writeBytes(newAddress, data);
						}
					} else {
						if (p_fragmentation[segment] > MAX_FRAGMENTATION) {
							newAddress = defragmentLevel0Table(address);
						}
					}

					if (newAddress != 0) {
						writeEntry(p_addressTable, i, newAddress + (entry & FULL_FLAG));
					}
				}
			} catch (final MemoryException e) {}
		}
//		writeUnlock(p_addressTable);
	}

	/**
	 * Defragments a level 0 table
	 * @param p_addressTable
	 *            the level 0 table to defragment
	 * @return the new table address
	 * @throws MemoryException
	 *             if the table could not be defragmented
	 */
	private long defragmentLevel0Table(final long p_addressTable) throws MemoryException {
		long ret;
		long table;
		long address;
		long[] addresses;
		byte[][] data;
		int[] sizes;
		int position;
		int entries;

		table = p_addressTable;
		entries = ENTRIES_PER_LID_LEVEL + 1;
		addresses = new long[entries];
		Arrays.fill(addresses, 0);
		data = new byte[entries][];
		sizes = new int[entries];
		Arrays.fill(sizes, 0);

//		writeLock(table);

		try {
			addresses[0] = table;
			data[0] = m_rawMemory.readBytes(table);
			sizes[0] = LID_TABLE_SIZE;
			for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
				position = i + 1;

				address = readEntry(table, i) & BITMASK_ADDRESS;
				if (address != 0) {
					addresses[position] = address;
					data[position] = m_rawMemory.readBytes(address);
					sizes[position] = data[position].length;
				}
			}

			m_rawMemory.free(addresses);
			addresses = m_rawMemory.malloc(sizes);

			table = addresses[0];
			m_rawMemory.writeBytes(table, data[0]);
			for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
				position = i + 1;

				address = addresses[position];
				if (address != 0) {
					writeEntry(table, i, address);

					m_rawMemory.writeBytes(address, data[position]);
				}
			}
		} finally {
//			writeUnlock(table);
		}

		ret = table;

		return ret;
	}

	/**
	 * Defragments all Tables
	 * @return the time of the defragmentation
	 * @throws MemoryException
	 *             if the tables could not be defragmented
	 */
	private long defragmentAll() throws MemoryException {
		long ret;
		double[] fragmentation;

		ret = System.nanoTime();

		fragmentation = m_rawMemory.getFragmentation();
		defragmentTable(m_nodeIDTableDirectory, LID_TABLE_LEVELS - 1, fragmentation);

		ret = System.nanoTime() - ret;

		return ret;
	}

}
