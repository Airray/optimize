package edu.duke.starfish.profile.profileinfo.setup;

public class JobTrackerInfo extends TrackerInfo {
	public JobTrackerInfo() {
	}

	public JobTrackerInfo(long internalId, String name, String hostName,
			int port) {
		super(internalId, name, hostName, port);
	}

	public JobTrackerInfo(JobTrackerInfo other) {
		super(other);
	}

	public int hashCode() {
		return super.hashCode();
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj)) {
			return false;
		}
		return (obj instanceof JobTrackerInfo);
	}

	public String toString() {
		return "JobTrackerInfo [Name=" + getName() + ", Host=" + getHostName()
				+ ", Port=" + getPort() + "]";
	}
}