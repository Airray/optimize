package edu.duke.starfish.profile.profileinfo.execution.mrtasks;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRCleanupAttemptInfo;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MRCleanupInfo extends MRTaskInfo {
	private List<MRCleanupAttemptInfo> attempts;

	public MRCleanupInfo() {
		this.attempts = new ArrayList(1);
	}

	public MRCleanupInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg) {
		super(internalId, execId, startTime, endTime, status, errorMsg);
		this.attempts = new ArrayList(1);
	}

	public MRCleanupInfo(MRCleanupInfo other) {
		super(other);
		this.attempts = new ArrayList(other.attempts.size());

		for (MRCleanupAttemptInfo attempt : other.attempts)
			addAttempt(new MRCleanupAttemptInfo(attempt));
	}

	public List<MRCleanupAttemptInfo> getAttempts() {
		return this.attempts;
	}

	public MRCleanupAttemptInfo getSuccessfulAttempt() {
		for (MRCleanupAttemptInfo attempt : this.attempts) {
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
		if (!(obj instanceof MRCleanupInfo))
			return false;
		MRCleanupInfo other = (MRCleanupInfo) obj;
		if (this.attempts == null) {
			if (other.attempts != null)
				return false;
		} else if (!this.attempts.equals(other.attempts))
			return false;
		return true;
	}

	public String toString() {
		return "MRCleanupInfo [TaskId=" + getExecId() + ", StartTime="
				+ getStartTime() + ", EndTime=" + getEndTime() + ", Status="
				+ getStatus() + ", NumAttempts=" + this.attempts.size() + "]";
	}

	public void addAttempt(MRCleanupAttemptInfo attempt) {
		this.hash = -1;
		this.attempts.add(attempt);
	}
}