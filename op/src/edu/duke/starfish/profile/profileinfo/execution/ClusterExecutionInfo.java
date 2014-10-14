package edu.duke.starfish.profile.profileinfo.execution;

import edu.duke.starfish.profile.profileinfo.ClusterInfo;
import java.util.Date;

public abstract class ClusterExecutionInfo extends ClusterInfo {
	private String execId;
	private Date startTime;
	private Date endTime;
	private MRExecutionStatus status;
	private String errorMsg;

	public ClusterExecutionInfo() {
		this.execId = null;
		this.startTime = null;
		this.endTime = null;
		this.status = null;
		this.errorMsg = null;
	}

	public ClusterExecutionInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg) {
		super(internalId);
		this.execId = execId;
		this.startTime = startTime;
		this.endTime = endTime;
		this.status = status;
		this.errorMsg = errorMsg;
	}

	public ClusterExecutionInfo(ClusterExecutionInfo other) {
		super(other);
		this.execId = other.execId;
		this.startTime = (other.startTime == null ? null : new Date(
				other.startTime.getTime()));

		this.endTime = (other.endTime == null ? null : new Date(
				other.endTime.getTime()));

		this.status = other.status;
		this.errorMsg = other.errorMsg;
	}

	public long getDuration() {
		if ((this.endTime != null) && (this.startTime != null)) {
			return this.endTime.getTime() - this.startTime.getTime();
		}
		return 0L;
	}

	public String getExecId() {
		return this.execId;
	}

	public Date getStartTime() {
		return this.startTime;
	}

	public Date getEndTime() {
		return this.endTime;
	}

	public MRExecutionStatus getStatus() {
		return this.status;
	}

	public String getErrorMsg() {
		return this.errorMsg;
	}

	public void setExecId(String execId) {
		this.hash = -1;
		this.execId = execId;
	}

	public void setStartTime(Date startTime) {
		this.hash = -1;
		this.startTime = startTime;
	}

	public void setEndTime(Date endTime) {
		this.hash = -1;
		this.endTime = endTime;
	}

	public void setStatus(MRExecutionStatus status) {
		this.hash = -1;
		this.status = status;
	}

	public void setErrorMsg(String errorMsg) {
		this.hash = -1;
		this.errorMsg = errorMsg;
	}

	public int hashCode() {
		if (this.hash == -1) {
			this.hash = super.hashCode();
			this.hash = (31 * this.hash + (this.endTime == null ? 0
					: this.endTime.hashCode()));
			this.hash = (37 * this.hash + (this.execId == null ? 0
					: this.execId.hashCode()));
			this.hash = (41 * this.hash + (this.startTime == null ? 0
					: this.startTime.hashCode()));
			this.hash = (43 * this.hash + (this.status == null ? 0
					: this.status.hashCode()));
		}
		return this.hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof ClusterExecutionInfo))
			return false;
		ClusterExecutionInfo other = (ClusterExecutionInfo) obj;
		if (this.endTime == null) {
			if (other.endTime != null)
				return false;
		} else if (!this.endTime.equals(other.endTime))
			return false;
		if (this.execId == null) {
			if (other.execId != null)
				return false;
		} else if (!this.execId.equals(other.execId))
			return false;
		if (this.startTime == null) {
			if (other.startTime != null)
				return false;
		} else if (!this.startTime.equals(other.startTime))
			return false;
		if (this.status == null) {
			if (other.status != null)
				return false;
		} else if (!this.status.equals(other.status))
			return false;
		return true;
	}
}