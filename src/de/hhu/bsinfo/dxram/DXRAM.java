
package de.hhu.bsinfo.dxram;

import java.net.InetSocketAddress;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.ManifestHelper;

/**
 * Main class/entry point for any application to work with DXRAM and its services.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class DXRAM {
	protected DXRAMEngine m_engine;

	/**
	 * Constructor
	 */
	public DXRAM() {
		m_engine = new DXRAMEngine();
	}

	/**
	 * Initialize the instance.
	 *
	 * @param p_autoShutdown True to have DXRAM shut down automatically when the application quits.
	 *                       If false, the caller has to take care of shutting down the instance by calling shutdown when done.
	 * @return True if initializing was successful, false otherwise.
	 */
	public boolean initialize(final boolean p_autoShutdown) {
		boolean ret = m_engine.init();
		if (!ret) {
			return false;
		}

		printNodeInfo();
		if (p_autoShutdown) {
			Runtime.getRuntime().addShutdownHook(new ShutdownThread(this));
		}
		postInit();

		return true;
	}

	/**
	 * Initialize the instance.
	 *
	 * @param p_configurationFile Absolute or relative path to a configuration file
	 * @return True if initializing was successful, false otherwise.
	 */
	public boolean initialize(final String p_configurationFile) {
		boolean ret = m_engine.init(p_configurationFile);
		if (ret) {
			printNodeInfo();
			postInit();
		}
		return ret;
	}

	/**
	 * Initialize the instance.
	 *
	 * @param p_autoShutdown       True to have DXRAM shut down automatically when the application quits.
	 *                             If false, the caller has to take care of shutting down the instance by calling shutdown when done.
	 * @param p_configurationFiles Absolute or relative path to a configuration file
	 * @return True if initializing was successful, false otherwise.
	 */
	public boolean initialize(final boolean p_autoShutdown, final String p_configurationFile) {
		boolean ret = initialize(p_configurationFile);
		if (ret & p_autoShutdown) {
			Runtime.getRuntime().addShutdownHook(new ShutdownThread(this));
		}
		if (ret) {
			printNodeInfo();
			postInit();
		}

		return ret;
	}

	/**
	 * Get a service from DXRAM.
	 *
	 * @param <T>     Type of service to get
	 * @param p_class Class of the service to get. If one service has multiple implementations, use
	 *                the common super class here.
	 * @return Service requested or null if the service is not enabled/available.
	 */
	public <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
		return m_engine.getService(p_class);
	}

	/**
	 * Shut down DXRAM. Call this if you have not enabled auto shutdown on init.
	 */
	public void shutdown() {
		preShutdown();
		m_engine.shutdown();
	}

	/**
	 * Print some information after init about our current node.
	 */
	private void printNodeInfo() {
		String str = ">>> DXRAM Node <<<\n";
		String buildDate = ManifestHelper.getProperty(getClass(), "BuildDate");
		if (buildDate != null) {
			str += "BuildDate: " + buildDate + "\n";
		}
		String buildUser = ManifestHelper.getProperty(getClass(), "BuildUser");
		if (buildUser != null) {
			str += "BuildUser: " + buildUser + "\n";
		}

		str += "Cwd: " + System.getProperty("user.dir") + "\n";

		BootService bootService = m_engine.getService(BootService.class);

		if (bootService != null) {
			short nodeId = bootService.getNodeID();
			str += "NodeID: " + NodeID.toHexString(nodeId) + "\n";
			str += "Role: " + bootService.getNodeRole(nodeId) + "\n";

			InetSocketAddress address = bootService.getNodeAddress(nodeId);
			str += "Address: " + address;

			System.out.println(str);
		}
	}

	/**
	 * Stub method for any class extending this class.
	 * Override this to run some tasks like initializing variables after
	 * DXRAM has booted.
	 */
	protected void postInit() {
		// stub
	}

	/**
	 * Stub method for any class extending this class.
	 * Override this to run cleanup before DXRAM shuts down.
	 */
	protected void preShutdown() {
		// stub
	}

	/**
	 * Shuts down DXRAM in case of the system exits
	 *
	 * @author Florian Klein 03.09.2013
	 */
	private static final class ShutdownThread extends Thread {

		private DXRAM m_dxram;

		/**
		 * Creates an instance of ShutdownThread
		 *
		 * @param p_dxram Reference to DXRAM instance.
		 */
		private ShutdownThread(final DXRAM p_dxram) {
			super(ShutdownThread.class.getSimpleName());
			m_dxram = p_dxram;
		}

		@Override
		public void run() {
			m_dxram.shutdown();
		}

	}
}
