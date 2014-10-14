package edu.duke.starfish.profile.profileinfo.execution.mrtasks;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.setup.SlaveHostInfo;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MRMapInfo extends MRTaskInfo {
	List<MRMapAttemptInfo> attempts;
	List<SlaveHostInfo> splitHosts;

	public MRMapInfo() {
		this.attempts = new ArrayList(1);
		this.splitHosts = new ArrayList(3);
	}

	public MRMapInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			List<SlaveHostInfo> splitHosts) {
		super(internalId, execId, startTime, endTime, status, errorMsg);
		this.attempts = new ArrayList(1);
		this.splitHosts = splitHosts;
	}

	public MRMapInfo(MRMapInfo other) {
		super(other);

		this.attempts = new ArrayList(other.attempts.size());
		for (MRMapAttemptInfo attempt : other.attempts) {
			addAttempt(new MRMapAttemptInfo(attempt));
		}
		this.splitHosts = new ArrayList(other.splitHosts.size());
		for (SlaveHostInfo host : other.splitHosts)
			addSplitHost(new SlaveHostInfo(host));
	}

	public List<MRMapAttemptInfo> getAttempts() {
		return this.attempts;
	}

	public List<SlaveHostInfo> getSplitHosts() {
		return this.splitHosts;
	}

	public MRMapAttemptInfo getSuccessfulAttempt() {
		for (MRMapAttemptInfo attempt : this.attempts) {
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
			this.hash = (37 * this.hash + (this.splitHosts == null ? 0
					: this.splitHosts.hashCode()));
		}

		return this.hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof MRMapInfo))
			return false;
		MRMapInfo other = (MRMapInfo) obj;
		if (this.attempts == null) {
			if (other.attempts != null)
				return false;
		} else if (!this.attempts.equals(other.attempts))
			return false;
		if (this.splitHosts == null) {
			if (other.splitHosts != null)
				return false;
		} else if (!this.splitHosts.equals(other.splitHosts))
			return false;
		return true;
	}

	public String toString() {
		String str = "MRMapInfo [TaskId=" + getExecId() + ", StartTime="
				+ getStartTime() + ", EndTime=" + getEndTime() + ", Status="
				+ getStatus() + ", NumAttempts=" + this.attempts.size()
				+ ", splitHosts=[";

		if (this.splitHosts != null) {
			for (SlaveHostInfo host : this.splitHosts)
				str = str + host.getName() + ",";
		} else {
			str = str + "null";
		}

		str = str + "]]";
		return str;
	}

	public void addAttempt(MRMapAttemptInfo attempt) {
		this.hash = -1;
		this.attempts.add(attempt);
	}

	public void addSplitHost(SlaveHostInfo host) {
		this.hash = -1;
		this.splitHosts.add(host);
	}

	public MRMapAttemptInfo findMRMapAttempt(String attemptId) {
		for (MRMapAttemptInfo mrMapAttempt : this.attempts) {
			if (mrMapAttempt.getExecId().equals(attemptId)) {
				return mrMapAttempt;
			}
		}

		return null;
	}
}