package edu.duke.starfish.profile.profileinfo.setup;

public abstract class TrackerInfo extends ClusterSetupInfo {
	private String hostName;
	private int port;

	public TrackerInfo() {
		this.hostName = null;
		this.port = 0;
	}

	public TrackerInfo(long internalId, String name, String hostName, int port) {
		super(internalId, name);
		this.hostName = hostName;
		this.port = port;
	}

	public TrackerInfo(TrackerInfo other) {
		super(other);
		this.hostName = other.hostName;
		this.port = other.port;
	}

	public int getPort() {
		return this.port;
	}

	public String getHostName() {
		return this.hostName;
	}

	public void setPort(int port) {
		this.hash = -1;
		this.port = port;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result
				+ (this.hostName == null ? 0 : this.hostName.hashCode());
		result = 37 * result + this.port;
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof TrackerInfo))
			return false;
		TrackerInfo other = (TrackerInfo) obj;
		if (this.hostName == null) {
			if (other.hostName != null)
				return false;
		} else if (!this.hostName.equals(other.hostName)) {
			return false;
		}
		return this.port == other.port;
	}
}