package edu.duke.starfish.profile.profileinfo.setup;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RackInfo extends ClusterSetupInfo {
	private MasterHostInfo masterHost;
	private Map<String, SlaveHostInfo> slaveHosts;

	public RackInfo() {
		this.masterHost = null;
		this.slaveHosts = new HashMap();
	}

	public RackInfo(long internalId, String name) {
		super(internalId, name);
		this.masterHost = null;
		this.slaveHosts = new HashMap();
	}

	public RackInfo(RackInfo other) {
		super(other);
		this.masterHost = null;
		if (other.masterHost != null)
			setMasterHost(new MasterHostInfo(other.masterHost));
		this.slaveHosts = new HashMap(other.slaveHosts.size());

		for (SlaveHostInfo slave : other.slaveHosts.values())
			addSlaveHost(new SlaveHostInfo(slave));
	}

	public MasterHostInfo getMasterHost() {
		return this.masterHost;
	}

	public SlaveHostInfo getSlaveHost(String slaveName) {
		return (SlaveHostInfo) this.slaveHosts.get(slaveName);
	}

	public Collection<SlaveHostInfo> getSlaveHosts() {
		return this.slaveHosts.values();
	}

	public void setMasterHost(MasterHostInfo host) {
		this.masterHost = host;
		host.setRackName(getName());
		this.hash = -1;
	}

	public void addSlaveHost(SlaveHostInfo host) {
		this.slaveHosts.put(host.getName(), host);
		host.setRackName(getName());
		this.hash = -1;
	}

	public int hashCode() {
		if (this.hash == -1) {
			this.hash = super.hashCode();
			this.hash = (31 * this.hash + (this.masterHost == null ? 0
					: this.masterHost.hashCode()));

			this.hash = (37 * this.hash + (this.slaveHosts == null ? 0
					: this.slaveHosts.hashCode()));
		}

		return this.hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof RackInfo))
			return false;
		RackInfo other = (RackInfo) obj;
		if (this.masterHost == null) {
			if (other.masterHost != null)
				return false;
		} else if (!this.masterHost.equals(other.masterHost))
			return false;
		if (this.slaveHosts == null) {
			if (other.slaveHosts != null)
				return false;
		} else if (!this.slaveHosts.equals(other.slaveHosts))
			return false;
		return true;
	}

	public String toString() {
		return new StringBuilder()
				.append("RackInfo [Name=")
				.append(getName())
				.append(", MasterHost=")
				.append(this.masterHost == null ? "NONE" : this.masterHost
						.getName()).append(", Hosts=")
				.append(this.slaveHosts.keySet()).append("]").toString();
	}
}