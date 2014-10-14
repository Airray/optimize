package edu.duke.starfish.profile.profileinfo.setup;

public abstract class HostInfo extends ClusterSetupInfo {
	private String ipAddress;
	private String rackName;

	public HostInfo() {
		this.ipAddress = null;
		this.rackName = null;
	}

	public HostInfo(long internalId, String name, String ipAddress,
			String rackName) {
		super(internalId, name);
		this.ipAddress = ipAddress;
		this.rackName = rackName;
	}

	public HostInfo(HostInfo other) {
		super(other);
		this.ipAddress = other.ipAddress;
		this.rackName = other.rackName;
	}

	public String getIpAddress() {
		return this.ipAddress;
	}

	public String getRackName() {
		return this.rackName;
	}

	public void setIpAddress(String ipAddress) {
		this.hash = -1;
		this.ipAddress = ipAddress;
	}

	public void setRackName(String rackName) {
		this.hash = -1;
		this.rackName = rackName;
	}

	public int hashCode() {
		if (this.hash == -1) {
			this.hash = super.hashCode();
			this.hash = (31 * this.hash + (this.ipAddress == null ? 0
					: this.ipAddress.hashCode()));
			this.hash = (37 * this.hash + (this.rackName == null ? 0
					: this.rackName.hashCode()));
		}
		return this.hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof HostInfo))
			return false;
		HostInfo other = (HostInfo) obj;
		if (this.ipAddress == null) {
			if (other.ipAddress != null)
				return false;
		} else if (!this.ipAddress.equals(other.ipAddress)) {
			return false;
		}

		if (this.rackName == null) {
			if (other.rackName != null)
				return false;
		} else if (!this.rackName.equals(other.rackName)) {
			return false;
		}
		return true;
	}
}