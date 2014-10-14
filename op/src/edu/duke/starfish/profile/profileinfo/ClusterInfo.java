package edu.duke.starfish.profile.profileinfo;

public abstract class ClusterInfo {
	private long internalId;
	protected int hash;

	public ClusterInfo() {
		this.internalId = -1L;
		this.hash = -1;
	}

	public ClusterInfo(long internalId) {
		this.internalId = internalId;
		this.hash = -1;
	}

	public ClusterInfo(ClusterInfo other) {
		this.internalId = other.internalId;
		this.hash = other.hash;
	}

	public long getInternalId() {
		return this.internalId;
	}

	public void setInternalId(long internalId) {
		this.hash = -1;
		this.internalId = internalId;
	}

	public int hashCode() {
		if (this.hash == -1)
			this.hash = (31 + (int) (this.internalId ^ this.internalId >>> 32));
		return this.hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof ClusterInfo))
			return false;
		ClusterInfo other = (ClusterInfo) obj;

		return this.internalId == other.internalId;
	}

	public abstract String toString();
}