package edu.duke.starfish.profile.profileinfo.execution.jobs;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRCleanupInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRMapInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRReduceInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRSetupInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.metrics.DataTransfer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class MRJobInfo extends JobInfo {
	private List<MRSetupInfo> setupTasks;
	private List<MRMapInfo> mapTasks;
	private List<MRReduceInfo> reduceTasks;
	private List<MRCleanupInfo> cleanupTasks;
	private List<DataTransfer> dataTransfers;
	private MRJobProfile profile;
	private MRJobProfile adjProfile;
	private boolean hasAdjProfile;
	private List<MRMapAttemptInfo> sucMapAttempts;
	private List<MRReduceAttemptInfo> sucReduceAttempts;
	private static final String JOB = "job";
	private static final String TASK = "task";
	private static final String USCORE = "_";

	public MRJobInfo() {
		this.cleanupTasks = new ArrayList(1);
		this.mapTasks = new ArrayList();
		this.reduceTasks = new ArrayList();
		this.setupTasks = new ArrayList(1);

		this.dataTransfers = new ArrayList(0);
		this.profile = null;
		this.adjProfile = null;
		this.hasAdjProfile = false;

		this.sucMapAttempts = null;
		this.sucReduceAttempts = null;
	}

	public MRJobInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			String name, String user) {
		super(internalId, execId, startTime, endTime, status, errorMsg, name,
				user);

		this.cleanupTasks = new ArrayList(1);
		this.mapTasks = new ArrayList();
		this.reduceTasks = new ArrayList();
		this.setupTasks = new ArrayList(1);

		this.dataTransfers = new ArrayList(0);
		this.profile = null;
		this.adjProfile = null;
		this.hasAdjProfile = false;

		this.sucMapAttempts = null;
		this.sucReduceAttempts = null;
	}

	public MRJobInfo(MRJobInfo other) {
		super(other);

		this.cleanupTasks = new ArrayList(other.cleanupTasks.size());
		for (MRCleanupInfo cleanup : other.cleanupTasks) {
			addCleanupTaskInfo(new MRCleanupInfo(cleanup));
		}
		this.mapTasks = new ArrayList(other.mapTasks.size());
		for (MRMapInfo map : other.mapTasks) {
			addMapTaskInfo(new MRMapInfo(map));
		}
		this.reduceTasks = new ArrayList(other.reduceTasks.size());
		for (MRReduceInfo reduce : other.reduceTasks) {
			addReduceTaskInfo(new MRReduceInfo(reduce));
		}
		this.setupTasks = new ArrayList(other.setupTasks.size());
		for (MRSetupInfo setup : other.setupTasks) {
			addSetupTaskInfo(new MRSetupInfo(setup));
		}
		this.dataTransfers = new ArrayList(other.dataTransfers.size());
		for (DataTransfer transfer : other.dataTransfers) {
			addDataTransfer(new DataTransfer(transfer));
		}
		this.profile = (other.profile == null ? null : new MRJobProfile(
				other.profile));

		this.adjProfile = (other.adjProfile == null ? null : new MRJobProfile(
				other.adjProfile));

		this.hasAdjProfile = other.hasAdjProfile;

		this.sucMapAttempts = null;
		this.sucReduceAttempts = null;
	}

	public List<MRSetupInfo> getSetupTasks() {
		return this.setupTasks;
	}

	public List<MRMapInfo> getMapTasks() {
		return this.mapTasks;
	}

	public List<MRReduceInfo> getReduceTasks() {
		return this.reduceTasks;
	}

	public List<MRCleanupInfo> getCleanupTasks() {
		return this.cleanupTasks;
	}

	public List<DataTransfer> getDataTransfers() {
		return this.dataTransfers;
	}

	public MRJobProfile getOrigProfile() {
		if (this.profile == null)
			this.profile = new MRJobProfile(getExecId());
		return this.profile;
	}

	public MRJobProfile getProfile() {
		if (this.hasAdjProfile)
			return this.adjProfile;
		if (this.profile == null)
			this.profile = new MRJobProfile(getExecId());
		return this.profile;
	}

	public void setProfile(MRJobProfile profile) {
		this.profile = profile;
	}

	public void setAdjProfile(MRJobProfile adjProfile) {
		this.adjProfile = adjProfile;
		this.hasAdjProfile = true;
	}

	public int hashCode() {
		if (this.hash == -1) {
			this.hash = super.hashCode();
			this.hash = (31 * this.hash + (this.cleanupTasks == null ? 0
					: this.cleanupTasks.hashCode()));

			this.hash = (37 * this.hash + (this.mapTasks == null ? 0
					: this.mapTasks.hashCode()));
			this.hash = (41 * this.hash + (this.reduceTasks == null ? 0
					: this.reduceTasks.hashCode()));

			this.hash = (43 * this.hash + (this.setupTasks == null ? 0
					: this.setupTasks.hashCode()));
		}

		return this.hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof MRJobInfo))
			return false;
		MRJobInfo other = (MRJobInfo) obj;
		if (this.cleanupTasks == null) {
			if (other.cleanupTasks != null)
				return false;
		} else if (!this.cleanupTasks.equals(other.cleanupTasks))
			return false;
		if (this.mapTasks == null) {
			if (other.mapTasks != null)
				return false;
		} else if (!this.mapTasks.equals(other.mapTasks))
			return false;
		if (this.reduceTasks == null) {
			if (other.reduceTasks != null)
				return false;
		} else if (!this.reduceTasks.equals(other.reduceTasks))
			return false;
		if (this.setupTasks == null) {
			if (other.setupTasks != null)
				return false;
		} else if (!this.setupTasks.equals(other.setupTasks))
			return false;
		return true;
	}

	public String toString() {
		return "MRJobInfo [ID=" + getExecId() + ", Name=" + getName()
				+ ", User=" + getUser() + ", StartTime=" + getStartTime()
				+ ", EndTime=" + getEndTime() + ", Status=" + getStatus()
				+ ", NumCleanupTasks=" + this.cleanupTasks.size()
				+ ", NumMapTasks=" + this.mapTasks.size() + ", NumReduceTasks="
				+ this.reduceTasks.size() + ", NumSetupTasks="
				+ this.setupTasks.size() + "]";
	}

	public void addDataTransfer(DataTransfer transfer) {
		this.dataTransfers.add(transfer);
	}

	public void addDataTransfers(Collection<DataTransfer> transfers) {
		this.dataTransfers.addAll(transfers);
	}

	public void addCleanupTaskInfo(MRCleanupInfo cleanupInfo) {
		this.hash = -1;
		this.cleanupTasks.add(cleanupInfo);
	}

	public void addMapTaskInfo(MRMapInfo mapInfo) {
		this.hash = -1;
		this.mapTasks.add(mapInfo);
	}

	public void addReduceTaskInfo(MRReduceInfo reduceInfo) {
		this.hash = -1;
		this.reduceTasks.add(reduceInfo);
	}

	public void addSetupTaskInfo(MRSetupInfo setupInfo) {
		this.hash = -1;
		this.setupTasks.add(setupInfo);
	}

	public void copyOtherJob(MRJobInfo other) {
		this.setupTasks = other.setupTasks;
		this.mapTasks = other.mapTasks;
		this.reduceTasks = other.reduceTasks;
		this.cleanupTasks = other.cleanupTasks;

		this.dataTransfers = other.dataTransfers;
		this.profile = other.profile;
		this.adjProfile = other.adjProfile;
		this.hasAdjProfile = other.hasAdjProfile;

		this.sucMapAttempts = other.sucMapAttempts;
		this.sucReduceAttempts = other.sucReduceAttempts;

		setName(other.getName());
		setUser(other.getUser());

		setExecId(other.getExecId());
		setStartTime(other.getStartTime());
		setEndTime(other.getEndTime());
		setStatus(other.getStatus());
		setErrorMsg(other.getErrorMsg());
		setInternalId(other.getInternalId());
	}

	public List<DataTransfer> getDataTransfersFromMap(
			MRMapAttemptInfo mrMapAttempt) {
		List result = new ArrayList(this.reduceTasks.size());

		for (DataTransfer dataTransfer : this.dataTransfers) {
			if (dataTransfer.getSource().equals(mrMapAttempt)) {
				result.add(dataTransfer);
			}
		}

		return result;
	}

	public List<DataTransfer> getDataTransfersToReduce(
			MRReduceAttemptInfo mrReduceAttempt) {
		List result = new ArrayList(this.mapTasks.size());
		for (DataTransfer dataTransfer : this.dataTransfers) {
			if (dataTransfer.getDestination().equals(mrReduceAttempt)) {
				result.add(dataTransfer);
			}
		}

		return result;
	}

	public List<MRMapAttemptInfo> getMapAttempts(MRExecutionStatus status) {
		if ((status == MRExecutionStatus.SUCCESS)
				&& (this.sucMapAttempts != null)) {
			return this.sucMapAttempts;
		}

		List mrMapAttempts = new ArrayList(this.mapTasks.size());

		for (MRMapInfo mrMap : this.mapTasks) {
			for (MRMapAttemptInfo mrMapAttempt : mrMap.getAttempts()) {
				if (mrMapAttempt.getStatus() == status) {
					mrMapAttempts.add(mrMapAttempt);
				}
			}

		}

		if (status == MRExecutionStatus.SUCCESS) {
			this.sucMapAttempts = mrMapAttempts;
		}
		return mrMapAttempts;
	}

	public List<MRReduceAttemptInfo> getReduceAttempts(MRExecutionStatus status) {
		if ((status == MRExecutionStatus.SUCCESS)
				&& (this.sucReduceAttempts != null)) {
			return this.sucReduceAttempts;
		}

		List mrReduceAttempts = new ArrayList(this.reduceTasks.size());

		for (MRReduceInfo mrReduce : this.reduceTasks) {
			for (MRReduceAttemptInfo mrReduceAttempt : mrReduce.getAttempts()) {
				if (mrReduceAttempt.getStatus() == status) {
					mrReduceAttempts.add(mrReduceAttempt);
				}
			}

		}

		if (status == MRExecutionStatus.SUCCESS) {
			this.sucReduceAttempts = mrReduceAttempts;
		}
		return mrReduceAttempts;
	}

	public MRMapAttemptInfo findMRMapAttempt(String attemptId) {
		String mapId = getTaskIdFromAttemptId(attemptId);
		for (MRMapInfo map : this.mapTasks) {
			if (map.getExecId().equals(mapId)) {
				return map.findMRMapAttempt(attemptId);
			}
		}
		return null;
	}

	public MRReduceAttemptInfo findMRReduceAttempt(String attemptId) {
		String reduceId = getTaskIdFromAttemptId(attemptId);
		for (MRReduceInfo reduce : this.reduceTasks) {
			if (reduce.getExecId().equals(reduceId)) {
				return reduce.findMRReduceAttempt(attemptId);
			}
		}
		return null;
	}

	public boolean isMapOnly() {
		return this.reduceTasks.isEmpty();
	}

	public static String getTaskIdFromAttemptId(String attemptId) {
		String[] pieces = attemptId.split("_");
		if ((pieces != null) && (pieces.length == 6)) {
			StringBuffer sb = new StringBuffer(32);
			sb.append("task");
			sb.append("_");
			sb.append(pieces[1]);
			sb.append("_");
			sb.append(pieces[2]);
			sb.append("_");
			sb.append(pieces[3]);
			sb.append("_");
			sb.append(pieces[4]);
			return sb.toString();
		}
		return null;
	}

	public static String getJobIdFromAttemptId(String attemptId) {
		String[] pieces = attemptId.split("_");
		if ((pieces != null) && (pieces.length == 6)) {
			StringBuffer sb = new StringBuffer(22);
			sb.append("job");
			sb.append("_");
			sb.append(pieces[1]);
			sb.append("_");
			sb.append(pieces[2]);
			return sb.toString();
		}
		return null;
	}
}