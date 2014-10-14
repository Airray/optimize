package edu.duke.starfish.profile.profileinfo.setup;

import edu.duke.starfish.profile.profileinfo.ClusterInfo;

public abstract class ClusterSetupInfo extends ClusterInfo {
	private String name;

	public ClusterSetupInfo() {
		this.name = null;
	}

	public ClusterSetupInfo(long internalId, String name) {
		super(internalId);
		this.name = name;
	}

	public ClusterSetupInfo(ClusterSetupInfo other) {
		super(other);
		this.name = other.name;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.hash = -1;
		this.name = name;
	}

	public int hashCode() {
		if (this.hash == -1) {
			this.hash = super.hashCode();
			this.hash = (31 * this.hash + (this.name == null ? 0 : this.name
					.hashCode()));
		}
		return this.hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof ClusterSetupInfo))
			return false;
		ClusterSetupInfo other = (ClusterSetupInfo) obj;
		if (this.name == null) {
			if (other.name != null)
				return false;
		} else if (!this.name.equals(other.name))
			return false;
		return true;
	}
}