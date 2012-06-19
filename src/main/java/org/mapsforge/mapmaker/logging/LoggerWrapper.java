package org.mapsforge.mapmaker.logging;

import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * This extension of Java's default logger provides {@link ProgressManager} integration that allows forwarding log
 * messages to a GUI. It allows simultaneous logging on an output stream as provided in the base Logger class and to a
 * GUI.
 * 
 * @author Karsten Groll
 */
public class LoggerWrapper extends Logger {
	private ProgressManager pm;
	private static ProgressManager defaultProgressManager;

	private LoggerWrapper(String name) {
		super(name, null);
	}

	/**
	 * Returns or creates a logger that forwards messages to a {@link ProgressManager}. To create a logger object with a
	 * progress manager use {@link #getLogger(String, ProgressManager)} instead.
	 * 
	 * @param name
	 *            The logger's unique name. By default the calling class' name is used.
	 * @return A logger that forwards messages to a {@link ProgressManager}.
	 */
	public synchronized static LoggerWrapper getLogger(String name) {
		System.out.println("getLogger: " + name);
		LogManager lm = LogManager.getLogManager();
		Logger l = lm.getLogger(name);

		if (l == null) {
			lm.addLogger(new LoggerWrapper(name));
			((LoggerWrapper) lm.getLogger(name)).setProgressManager(LoggerWrapper.defaultProgressManager);
		}

		return (LoggerWrapper) lm.getLogger(name);
	}

	/**
	 * Returns or creates a logger that forwards messages to a {@link ProgressManager}. If the logger does not yet have
	 * any progress manager assigned, the given one will be used.
	 * 
	 * @param name
	 *            The logger's unique name. By default the calling class' name is used.
	 * @param pm
	 *            The logger's progress manager. This value is only used if a logger object has to be created or a
	 *            logger with a given name does not yet have any progress manager assigned or if the default progress
	 *            manager is used.
	 * @return A logger that forwards messages to a {@link ProgressManager}.
	 */
	public static LoggerWrapper getLogger(String name, ProgressManager pm) {
		LoggerWrapper ret = getLogger(name);

		if (ret.getProgressManager() == null || ret.getProgressManager() == LoggerWrapper.defaultProgressManager) {
			ret.setProgressManager(pm);
		}

		return ret;
	}

	/**
	 * This method sets the logger's progress manager that receives certain log messages and forwards them to a GUI.
	 * Call this method to override the current {@link ProgressManager}.
	 * 
	 * @param pm
	 *            The progress manager to be used.
	 */
	public synchronized void setProgressManager(ProgressManager pm) {
		this.pm = pm;
	}

	public static void setDefaultProgressManager(ProgressManager pm) {
		LoggerWrapper.defaultProgressManager = pm;
	}

	private synchronized ProgressManager getProgressManager() {
		return this.pm;
	}

	@Override
	public synchronized void info(String msg) {
		super.info(msg);
		pm.sendMessage(msg);
	}
}
