package edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts;

import edu.duke.starfish.profile.profileinfo.execution.ClusterExecutionInfo;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;
import java.util.Date;

public abstract class MRTaskAttemptInfo extends ClusterExecutionInfo {
	private TaskTrackerInfo taskTracker;

	public MRTaskAttemptInfo() {
		this.taskTracker = null;
	}

	public MRTaskAttemptInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			TaskTrackerInfo taskTracker) {
		super(internalId, execId, startTime, endTime, status, errorMsg);
		this.taskTracker = taskTracker;
	}

	public MRTaskAttemptInfo(MRTaskAttemptInfo other) {
		super(other);
		this.taskTracker = (other.taskTracker == null ? null
				: new TaskTrackerInfo(other.taskTracker));
	}

	public TaskTrackerInfo getTaskTracker() {
		return this.taskTracker;
	}

	public void setTaskTracker(TaskTrackerInfo taskTracker) {
		this.hash = -1;
		this.taskTracker = taskTracker;
	}

	public String getTruncatedTaskId() {
		String taskId = getExecId();
		if (taskId == null) {
			return "";
		}

		int index = taskId.indexOf("_m_");
		if (index != -1) {
			return taskId.substring(index + 1);
		}
		index = taskId.indexOf("_r_");
		if (index != -1) {
			return taskId.substring(index + 1);
		}
		return taskId;
	}

	public abstract MRTaskProfile getProfile();

	public abstract void setProfile(MRTaskProfile paramMRTaskProfile);

	public int hashCode() {
		if (this.hash == -1) {
			this.hash = super.hashCode();
			this.hash = (31 * this.hash + (this.taskTracker == null ? 0
					: this.taskTracker.hashCode()));
		}

		return this.hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof MRTaskAttemptInfo))
			return false;
		MRTaskAttemptInfo other = (MRTaskAttemptInfo) obj;
		if (this.taskTracker == null) {
			if (other.taskTracker != null)
				return false;
		} else if (!this.taskTracker.equals(other.taskTracker))
			return false;
		return true;
	}
}