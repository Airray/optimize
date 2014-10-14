package edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts;

import edu.duke.starfish.profile.profileinfo.execution.DataLocality;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;
import java.util.Date;

public class MRMapAttemptInfo extends MRTaskAttemptInfo {
	private DataLocality dataLocality;
	private MRMapProfile profile;

	public MRMapAttemptInfo() {
		this.dataLocality = null;
		this.profile = null;
	}

	public MRMapAttemptInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			TaskTrackerInfo taskTracker, DataLocality dataLocality) {
		super(internalId, execId, startTime, endTime, status, errorMsg,
				taskTracker);

		this.dataLocality = dataLocality;
		this.profile = null;
	}

	public MRMapAttemptInfo(MRMapAttemptInfo other) {
		super(other);
		this.dataLocality = other.dataLocality;
		this.profile = (other.profile == null ? null : new MRMapProfile(
				other.profile));
	}

	public DataLocality getDataLocality() {
		return this.dataLocality;
	}

	public void setDataLocality(DataLocality dataLocality) {
		this.hash = -1;
		this.dataLocality = dataLocality;
	}

	public MRMapProfile getProfile() {
		if (this.profile == null)
			this.profile = new MRMapProfile(getExecId());
		return this.profile;
	}

	public void setProfile(MRTaskProfile profile) {
		this.profile = ((MRMapProfile) profile);
	}

	public int hashCode() {
		if (this.hash == -1) {
			this.hash = super.hashCode();
			this.hash = (31 * this.hash + (this.dataLocality == null ? 0
					: this.dataLocality.hashCode()));
		}

		return this.hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof MRMapAttemptInfo))
			return false;
		MRMapAttemptInfo other = (MRMapAttemptInfo) obj;
		if (this.dataLocality == null) {
			if (other.dataLocality != null)
				return false;
		} else if (!this.dataLocality.equals(other.dataLocality))
			return false;
		return true;
	}

	public String toString() {
		return new StringBuilder()
				.append("MRMapAttemptInfo [ID=")
				.append(getExecId())
				.append(", StartTime=")
				.append(getStartTime())
				.append(", EndTime=")
				.append(getEndTime())
				.append(", TaskTracker=")
				.append(getTaskTracker() == null ? "null" : getTaskTracker()
						.getName()).append(", Status=").append(getStatus())
				.append(", Locality=").append(this.dataLocality).append("]")
				.toString();
	}
}