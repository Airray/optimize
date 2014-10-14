package edu.duke.starfish.profile.profileinfo.execution.jobs;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PigJobInfo extends JobInfo {
	private List<MRJobInfo> mrJobs;

	public PigJobInfo() {
		this.mrJobs = new ArrayList();
	}

	public PigJobInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			String name, String user, List<MRJobInfo> mrJobs) {
		super(internalId, execId, startTime, endTime, status, errorMsg, name,
				user);

		this.mrJobs = mrJobs;
	}

	public PigJobInfo(PigJobInfo other) {
		this.mrJobs = new ArrayList(other.mrJobs.size());
		for (MRJobInfo mrJob : other.mrJobs)
			addMRJobInfo(new MRJobInfo(mrJob));
	}

	public List<MRJobInfo> getMrJobs() {
		return this.mrJobs;
	}

	public int hashCode() {
		if (this.hash == -1) {
			this.hash = super.hashCode();
			this.hash = (31 * this.hash + (this.mrJobs == null ? 0
					: this.mrJobs.hashCode()));
		}
		return this.hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof PigJobInfo))
			return false;
		PigJobInfo other = (PigJobInfo) obj;
		if (this.mrJobs == null) {
			if (other.mrJobs != null)
				return false;
		} else if (!this.mrJobs.equals(other.mrJobs))
			return false;
		return true;
	}

	public String toString() {
		return new StringBuilder().append("PigJobInfo [ID=")
				.append(getExecId()).append(", Name=").append(getName())
				.append(", User=").append(getUser()).append(", StartTime=")
				.append(getStartTime()).append(", EndTime=")
				.append(getEndTime()).append(", Status=").append(getStatus())
				.append(", NumMRJobs=")
				.append(this.mrJobs == null ? 0 : this.mrJobs.size())
				.append("]").toString();
	}

	public void addMRJobInfo(MRJobInfo mrJobInfo) {
		if (this.mrJobs == null) {
			this.mrJobs = new ArrayList();
		}

		this.hash = -1;
		this.mrJobs.add(mrJobInfo);
	}
}