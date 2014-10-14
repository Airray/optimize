package edu.duke.starfish.profile.profileinfo.execution.mrtasks;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRSetupAttemptInfo;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MRSetupInfo extends MRTaskInfo {
	private List<MRSetupAttemptInfo> attempts;

	public MRSetupInfo() {
		this.attempts = new ArrayList(1);
	}

	public MRSetupInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg) {
		super(internalId, execId, startTime, endTime, status, errorMsg);
		this.attempts = new ArrayList(1);
	}

	public MRSetupInfo(MRSetupInfo other) {
		super(other);
		this.attempts = new ArrayList(other.attempts.size());
		for (MRSetupAttemptInfo attempt : other.attempts)
			addAttempt(new MRSetupAttemptInfo(attempt));
	}

	public List<MRSetupAttemptInfo> getAttempts() {
		return this.attempts;
	}

	public MRSetupAttemptInfo getSuccessfulAttempt() {
		for (MRSetupAttemptInfo attempt : this.attempts) {
			if (attempt.getStatus() == MRExecutionStatus.SUCCESS)
				return attempt;
		}
		return null;
	}

	public int hashCode() {
		if (this.hash == -1) {
			this.hash = super.hashCode();
			this.hash = (31 * this.hash + (this.attempts == null ? 0
					: this.attempts.hashCode()));
		}
		return this.hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof MRSetupInfo))
			return false;
		MRSetupInfo other = (MRSetupInfo) obj;
		if (this.attempts == null) {
			if (other.attempts != null)
				return false;
		} else if (!this.attempts.equals(other.attempts))
			return false;
		return true;
	}

	public String toString() {
		return "MRSetupInfo [TaskId=" + getExecId() + ", StartTime="
				+ getStartTime() + ", EndTime=" + getEndTime() + ", Status="
				+ getStatus() + ", NumAttempts=" + this.attempts.size() + "]";
	}

	public void addAttempt(MRSetupAttemptInfo attempt) {
		this.hash = -1;
		this.attempts.add(attempt);
	}
}