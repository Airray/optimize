package edu.duke.starfish.profile.profileinfo.execution.jobs;

import edu.duke.starfish.profile.profileinfo.execution.ClusterExecutionInfo;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import java.util.Date;

public abstract class JobInfo extends ClusterExecutionInfo {
	private String name;
	private String user;

	public JobInfo() {
		this.name = null;
		this.user = null;
	}

	public JobInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			String name, String user) {
		super(internalId, execId, startTime, endTime, status, errorMsg);
		this.name = name;
		this.user = user;
	}

	public JobInfo(JobInfo other) {
		super(other);
		this.name = other.name;
		this.user = other.user;
	}

	public String getName() {
		return this.name;
	}

	public String getUser() {
		return this.user;
	}

	public void setName(String name) {
		this.hash = -1;
		this.name = name;
	}

	public void setUser(String user) {
		this.hash = -1;
		this.user = user;
	}

	public int hashCode() {
		if (this.hash == -1) {
			this.hash = super.hashCode();
			this.hash = (31 * this.hash + (this.name == null ? 0 : this.name
					.hashCode()));
			this.hash = (37 * this.hash + (this.user == null ? 0 : this.user
					.hashCode()));
		}
		return this.hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof JobInfo))
			return false;
		JobInfo other = (JobInfo) obj;
		if (this.name == null) {
			if (other.name != null)
				return false;
		} else if (!this.name.equals(other.name))
			return false;
		if (this.user == null) {
			if (other.user != null)
				return false;
		} else if (!this.user.equals(other.user))
			return false;
		return true;
	}
}