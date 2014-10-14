package edu.duke.starfish.profile.profileinfo.setup;

public class SlaveHostInfo extends HostInfo {
	private TaskTrackerInfo taskTracker;

	public SlaveHostInfo() {
		this.taskTracker = null;
	}

	public SlaveHostInfo(long internalId, String name, String ipAddress,
			String rackName) {
		super(internalId, name, ipAddress, rackName);
		this.taskTracker = null;
	}

	public SlaveHostInfo(SlaveHostInfo other) {
		super(other);
		this.taskTracker = null;
		if (other.taskTracker != null)
			setTaskTracker(new TaskTrackerInfo(other.taskTracker));
	}

	public TaskTrackerInfo getTaskTracker() {
		return this.taskTracker;
	}

	public void setTaskTracker(TaskTrackerInfo taskTracker) {
		this.taskTracker = taskTracker;
		taskTracker.setHostName(getName());
	}

	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result
				+ (this.taskTracker == null ? 0 : this.taskTracker.hashCode());

		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof SlaveHostInfo))
			return false;
		SlaveHostInfo other = (SlaveHostInfo) obj;
		if (this.taskTracker == null) {
			if (other.taskTracker != null)
				return false;
		} else if (!this.taskTracker.equals(other.taskTracker))
			return false;
		return true;
	}

	public String toString() {
		return "SlaveHostInfo [Name=" + getName() + ", IPAddress="
				+ getIpAddress() + ", Rack=" + getRackName() + "]";
	}
}