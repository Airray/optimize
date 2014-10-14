package edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;
import java.util.Date;

public class MRReduceAttemptInfo extends MRTaskAttemptInfo {
	private Date shuffleEndTime;
	private Date sortEndTime;
	private MRReduceProfile profile;

	public MRReduceAttemptInfo() {
		this.shuffleEndTime = null;
		this.sortEndTime = null;
		this.profile = null;
	}

	public MRReduceAttemptInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			TaskTrackerInfo taskTracker, Date shuffleEndTime, Date sortEndTime) {
		super(internalId, execId, startTime, endTime, status, errorMsg,
				taskTracker);

		this.shuffleEndTime = shuffleEndTime;
		this.sortEndTime = sortEndTime;
		this.profile = null;
	}

	public MRReduceAttemptInfo(MRReduceAttemptInfo other) {
		super(other);
		this.shuffleEndTime = (this.shuffleEndTime == null ? null : new Date(
				other.shuffleEndTime.getTime()));

		this.sortEndTime = (this.sortEndTime == null ? null : new Date(
				other.sortEndTime.getTime()));

		this.profile = (other.profile == null ? null : new MRReduceProfile(
				other.profile));
	}

	public long getShuffleDuration() {
		if ((this.shuffleEndTime != null) && (getStartTime() != null)) {
			return this.shuffleEndTime.getTime() - getStartTime().getTime();
		}
		return 0L;
	}

	public long getSortDuration() {
		if ((this.sortEndTime != null) && (this.shuffleEndTime != null)) {
			return this.sortEndTime.getTime() - this.shuffleEndTime.getTime();
		}
		return 0L;
	}

	public long getReduceDuration() {
		if ((getEndTime() != null) && (this.sortEndTime != null)) {
			return getEndTime().getTime() - this.sortEndTime.getTime();
		}
		return 0L;
	}

	public Date getShuffleEndTime() {
		return this.shuffleEndTime;
	}

	public Date getSortEndTime() {
		return this.sortEndTime;
	}

	public void setShuffleEndTime(Date shuffleEndTime) {
		this.hash = -1;
		this.shuffleEndTime = shuffleEndTime;
	}

	public void setSortEndTime(Date sortEndTime) {
		this.hash = -1;
		this.sortEndTime = sortEndTime;
	}

	public MRReduceProfile getProfile() {
		if (this.profile == null)
			this.profile = new MRReduceProfile(getExecId());
		return this.profile;
	}

	public void setProfile(MRTaskProfile profile) {
		this.profile = ((MRReduceProfile) profile);
	}

	public int hashCode() {
		if (this.hash == -1) {
			this.hash = super.hashCode();
			this.hash = (31 * this.hash + (this.shuffleEndTime == null ? 0
					: this.shuffleEndTime.hashCode()));

			this.hash = (37 * this.hash + (this.sortEndTime == null ? 0
					: this.sortEndTime.hashCode()));
		}

		return this.hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof MRReduceAttemptInfo))
			return false;
		MRReduceAttemptInfo other = (MRReduceAttemptInfo) obj;
		if (this.shuffleEndTime == null) {
			if (other.shuffleEndTime != null)
				return false;
		} else if (!this.shuffleEndTime.equals(other.shuffleEndTime))
			return false;
		if (this.sortEndTime == null) {
			if (other.sortEndTime != null)
				return false;
		} else if (!this.sortEndTime.equals(other.sortEndTime))
			return false;
		return true;
	}

	public String toString() {
		return new StringBuilder()
				.append("MRReduceAttemptInfo [ID=")
				.append(getExecId())
				.append(", StartTime=")
				.append(getStartTime())
				.append(", EndTime=")
				.append(getEndTime())
				.append(", TaskTracker=")
				.append(getTaskTracker() == null ? "null" : getTaskTracker()
						.getName()).append(", Status=").append(getStatus())
				.append(", ShuffleEndTime=").append(this.shuffleEndTime)
				.append(", SortEndTime=").append(this.sortEndTime).append("]")
				.toString();
	}
}