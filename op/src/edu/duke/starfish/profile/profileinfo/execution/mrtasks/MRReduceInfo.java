package edu.duke.starfish.profile.profileinfo.execution.mrtasks;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MRReduceInfo extends MRTaskInfo {
	private List<MRReduceAttemptInfo> attempts;

	public MRReduceInfo() {
		this.attempts = new ArrayList(1);
	}

	public MRReduceInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg) {
		super(internalId, execId, startTime, endTime, status, errorMsg);
		this.attempts = new ArrayList(1);
	}

	public MRReduceInfo(MRReduceInfo other) {
		super(other);
		this.attempts = new ArrayList(other.attempts.size());

		for (MRReduceAttemptInfo attempt : other.attempts)
			addAttempt(new MRReduceAttemptInfo(attempt));
	}

	public List<MRReduceAttemptInfo> getAttempts() {
		return this.attempts;
	}

	public MRReduceAttemptInfo getSuccessfulAttempt() {
		for (MRReduceAttemptInfo attempt : this.attempts) {
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
		if (!(obj instanceof MRReduceInfo))
			return false;
		MRReduceInfo other = (MRReduceInfo) obj;
		if (this.attempts == null) {
			if (other.attempts != null)
				return false;
		} else if (!this.attempts.equals(other.attempts))
			return false;
		return true;
	}

	public String toString() {
		return "MRReduceInfo [TaskId=" + getExecId() + ", StartTime="
				+ getStartTime() + ", EndTime=" + getEndTime() + ", Status="
				+ getStatus() + ", NumAttempts=" + this.attempts.size() + "]";
	}

	public void addAttempt(MRReduceAttemptInfo attempt) {
		this.hash = -1;
		this.attempts.add(attempt);
	}

	public MRReduceAttemptInfo findMRReduceAttempt(String attemptId) {
		for (MRReduceAttemptInfo mrReduceAttempt : this.attempts) {
			if (mrReduceAttempt.getExecId().equals(attemptId)) {
				return mrReduceAttempt;
			}
		}

		return null;
	}
}