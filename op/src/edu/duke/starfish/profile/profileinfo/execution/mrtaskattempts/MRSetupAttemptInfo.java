package edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;
import java.util.Date;

public class MRSetupAttemptInfo extends MRTaskAttemptInfo {
	public MRSetupAttemptInfo() {
	}

	public MRSetupAttemptInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			TaskTrackerInfo taskTracker) {
		super(internalId, execId, startTime, endTime, status, errorMsg,
				taskTracker);
	}

	public MRSetupAttemptInfo(MRSetupAttemptInfo other) {
		super(other);
	}

	public MRTaskProfile getProfile() {
		return new MRMapProfile(getExecId());
	}

	public void setProfile(MRTaskProfile profile) {
	}

	public int hashCode() {
		return super.hashCode();
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj)) {
			return false;
		}
		return (obj instanceof MRSetupAttemptInfo);
	}

	public String toString() {
		return new StringBuilder()
				.append("MRSetupAttemptInfo [ID=")
				.append(getExecId())
				.append(", StartTime=")
				.append(getStartTime())
				.append(", EndTime=")
				.append(getEndTime())
				.append(", TaskTracker=")
				.append(getTaskTracker() == null ? "null" : getTaskTracker()
						.getName()).append(", Status=").append(getStatus())
				.append("]").toString();
	}
}