package edu.duke.starfish.profile.profileinfo.setup;

public class MasterHostInfo extends HostInfo {
	private JobTrackerInfo jobTracker;

	public MasterHostInfo() {
		this.jobTracker = null;
	}

	public MasterHostInfo(long internalId, String name, String ipAddress,
			String rackName) {
		super(internalId, name, ipAddress, rackName);
		this.jobTracker = null;
	}

	public MasterHostInfo(MasterHostInfo other) {
		super(other);
		this.jobTracker = null;
		if (other.jobTracker != null)
			setJobTracker(new JobTrackerInfo(other.jobTracker));
	}

	public JobTrackerInfo getJobTracker() {
		return this.jobTracker;
	}

	public void setJobTracker(JobTrackerInfo jobTracker) {
		this.jobTracker = jobTracker;
		jobTracker.setHostName(getName());
	}

	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result
				+ (this.jobTracker == null ? 0 : this.jobTracker.hashCode());

		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof MasterHostInfo))
			return false;
		MasterHostInfo other = (MasterHostInfo) obj;
		if (this.jobTracker == null) {
			if (other.jobTracker != null)
				return false;
		} else if (!this.jobTracker.equals(other.jobTracker))
			return false;
		return true;
	}

	public String toString() {
		return "MasterHostInfo [Name=" + getName() + ", IPAddress="
				+ getIpAddress() + ", Rack=" + getRackName() + "]";
	}
}