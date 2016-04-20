
package de.hhu.bsinfo.dxcompute.ms;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.utils.Pair;

public class Task {

	private AbstractTaskPayload m_payload;
	private String m_name;
	private short m_nodeIdSubmitted;
	private ArrayList<Pair<Short, Integer>> m_executionResults;
	private ArrayList<TaskListener> m_completionListeners = new ArrayList<TaskListener>();
	DXRAMServiceAccessor m_serviceAccessor;

	// private TaskStatisticsRecorderIDs m_statisticsRecorderIDs;

	public Task(final AbstractTaskPayload p_payload, final String p_name) {
		m_payload = p_payload;
		m_name = p_name;
	}

	public String getName() {
		return m_name;
	}

	/**
	 * Get the node id which submitted the task.
	 * @return Node id that submitted this task.
	 */
	public short getNodeIdSubmitted() {
		return m_nodeIdSubmitted;
	}

	public boolean hasTaskExecutionCompleted() {
		return m_executionResults != null;
	}

	public ArrayList<Pair<Short, Integer>> getTaskExecutionResults() {
		return m_executionResults;
	}

	public void registerTaskListener(final TaskListener p_listener) {
		m_completionListeners.add(p_listener);
	}

	@Override
	public String toString() {
		return "Task[" + m_name + "]: " + m_payload;
	}

	// /**
	// * Register statistics to be recorded by the task itself (task only).
	// */
	// private void registerStatisticsOperations() {
	// m_statisticsRecorderIDs = new TaskStatisticsRecorderIDs();
	// m_statisticsRecorderIDs.m_id = m_statisticsService.createRecorder(this.getClass());
	//
	// m_statisticsRecorderIDs.m_operations.m_execute = m_statisticsService
	// .createOperation(m_statisticsRecorderIDs.m_id, TaskStatisticsRecorderIDs.Operations.MS_EXECUTE);
	// }

	AbstractTaskPayload getPayload() {
		return m_payload;
	}

	void setNodeIdSubmitted(final short p_nodeId) {
		m_nodeIdSubmitted = p_nodeId;
	}

	void setTaskExecutionResults(final ArrayList<Pair<Short, Integer>> p_results) {
		m_executionResults = p_results;
	}

	void notifyListenersExecutionStarts() {
		for (TaskListener listener : m_completionListeners) {
			listener.taskBeforeExecution(this);
		}
	}

	void notifyListenersExecutionCompleted() {
		for (TaskListener listener : m_completionListeners) {
			listener.taskCompleted(this);
		}
	}
}