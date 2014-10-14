package edu.duke.starfish.profile.profileinfo;

import edu.duke.starfish.profile.profileinfo.setup.HostInfo;
import edu.duke.starfish.profile.profileinfo.setup.JobTrackerInfo;
import edu.duke.starfish.profile.profileinfo.setup.MasterHostInfo;
import edu.duke.starfish.profile.profileinfo.setup.RackInfo;
import edu.duke.starfish.profile.profileinfo.setup.SlaveHostInfo;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;
import edu.duke.starfish.profile.utils.ProfileUtils;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobTracker;

public class ClusterConfiguration {
	private static final Log LOG = LogFactory
			.getLog(ClusterConfiguration.class);
	private String name;
	private Map<String, RackInfo> racks;
	private MasterHostInfo masterHost;
	private Map<String, SlaveHostInfo> slaveHosts;
	private JobTrackerInfo jobTracker;
	private Map<String, TaskTrackerInfo> taskTrackers;
	private static final String MASTER_RACK = "master-rack";
	private static final String DEFAULT_RACK = "default-rack";
	private static final String JOB_TRACKER = "job_tracker_";
	private static final String TRACKER = "tracker_";
	private static final String LOCALHOST = "localhost";
	private static final String TAB = "\t";

	public ClusterConfiguration() {
		this.racks = new HashMap();
		this.masterHost = null;
		this.slaveHosts = new HashMap();
		this.jobTracker = null;
		this.taskTrackers = new HashMap();
		this.name = null;
	}

	public ClusterConfiguration(Configuration conf) {
		this();

		InetSocketAddress jobTrackerAddr = JobTracker.getAddress(conf);
		RackInfo masterRack = new RackInfo(0L, "master-rack");
		this.racks.put(masterRack.getName(), masterRack);
		this.masterHost = new MasterHostInfo(0L, jobTrackerAddr.getHostName(),
				jobTrackerAddr.getAddress().getHostAddress(),
				masterRack.getName());

		this.jobTracker = new JobTrackerInfo(0L, "job_tracker_"
				+ this.masterHost.getName(), this.masterHost.getName(),
				jobTrackerAddr.getPort());

		masterRack.setMasterHost(this.masterHost);
		this.masterHost.setJobTracker(this.jobTracker);

		this.name = conf.get("starfish.profiler.cluster.name");
		if (this.name == null) {
			this.name = this.masterHost.getName();
		}

		JobClient client = null;
		ClusterStatus cluster = null;
		try {
			client = new JobClient(jobTrackerAddr, conf);
			cluster = client.getClusterStatus(true);
		} catch (IOException e) {
			LOG.error("Unable to get cluster information from JobTracker", e);
			return;
		}

		int mapSlots = cluster.getMaxMapTasks() / cluster.getTaskTrackers();
		int redSlots = cluster.getMaxReduceTasks() / cluster.getTaskTrackers();
		long mapTaskMem = ProfileUtils.getMapTaskMemory(conf);
		long redTaskMem = ProfileUtils.getReduceTaskMemory(conf);

		int id = 1;
		for (String tracker : cluster.getActiveTrackerNames()) {
			String[] trackerPieces = tracker.split(":");
			if (trackerPieces.length != 3) {
				throw new RuntimeException(
						"ERROR: The tracker name should be of the form 'tracker_name:host_name:port' and not "
								+ tracker);
			}

			String hostInfo = trackerPieces[1];

			String[] hostPieces = hostInfo.split("/");
			String hostIpAddr;
			if (hostPieces.length == 3) {
				String rackName = hostPieces[0];
				String hostName = hostPieces[1];
				hostIpAddr = hostPieces[2];
			} else {
				String hostIpAddr;
				if (hostPieces.length == 2) {
					String rackName = "default-rack";
					String hostName = hostPieces[0];
					hostIpAddr = hostPieces[1];
				} else {
					throw new RuntimeException(
							"ERROR: The host name should be of the form '[rack_name/]host_name/ip_addr' and not "
									+ hostInfo);
				}
			}
			String hostIpAddr;
			String hostName;
			String rackName;
			String trackerName = trackerPieces[0];
			if ((hostName.contains("localhost"))
					&& (trackerName.startsWith("tracker_"))) {
				hostName = trackerName.substring(8);
			}

			RackInfo rack = addFindRackInfo(rackName);
			SlaveHostInfo host = new SlaveHostInfo(id, hostName, hostIpAddr,
					rack.getName());

			TaskTrackerInfo taskTracker = new TaskTrackerInfo(id, trackerName,
					host.getName(), Integer.parseInt(trackerPieces[2]),
					mapSlots, redSlots, mapTaskMem, redTaskMem);

			this.slaveHosts.put(host.getName(), host);
			this.taskTrackers.put(taskTracker.getName(), taskTracker);
			rack.addSlaveHost(host);
			host.setTaskTracker(taskTracker);

			id++;
		}
	}

	public ClusterConfiguration(ClusterConfiguration other) {
		this();

		this.name = other.name;
		this.racks = new HashMap(other.racks.size());

		for (RackInfo rack : other.racks.values())
			addRackInfo(new RackInfo(rack));
	}

	public int hashCode() {
		int result = 1;
		result = 31 * result + (this.racks == null ? 0 : this.racks.hashCode());
		result = 37 * result
				+ (this.masterHost == null ? 0 : this.masterHost.hashCode());

		result = 41 * result
				+ (this.slaveHosts == null ? 0 : this.slaveHosts.hashCode());

		result = 43 * result
				+ (this.jobTracker == null ? 0 : this.jobTracker.hashCode());

		result = 47
				* result
				+ (this.taskTrackers == null ? 0 : this.taskTrackers.hashCode());

		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof ClusterConfiguration))
			return false;
		ClusterConfiguration other = (ClusterConfiguration) obj;
		if (this.slaveHosts == null) {
			if (other.slaveHosts != null)
				return false;
		} else if (!this.slaveHosts.equals(other.slaveHosts))
			return false;
		if (this.masterHost == null) {
			if (other.masterHost != null)
				return false;
		} else if (!this.masterHost.equals(other.masterHost))
			return false;
		if (this.jobTracker == null) {
			if (other.jobTracker != null)
				return false;
		} else if (!this.jobTracker.equals(other.jobTracker))
			return false;
		if (this.racks == null) {
			if (other.racks != null)
				return false;
		} else if (!this.racks.equals(other.racks))
			return false;
		if (this.taskTrackers == null) {
			if (other.taskTrackers != null)
				return false;
		} else if (!this.taskTrackers.equals(other.taskTrackers))
			return false;
		return true;
	}

	public String toString() {
		return "ClusterConfiguration [Name=" + this.name + " Racks="
				+ getAllRackInfos() + ", jobTracker=" + getJobTrackerInfo()
				+ ", taskTrackers=" + getAllTaskTrackersInfos() + "]";
	}

	public void addRackInfo(RackInfo rack) {
		this.racks.put(rack.getName(), rack);

		for (SlaveHostInfo host : rack.getSlaveHosts()) {
			this.slaveHosts.put(host.getName(), host);

			TaskTrackerInfo taskTracker = host.getTaskTracker();
			if (taskTracker != null) {
				this.taskTrackers.put(taskTracker.getName(), taskTracker);
			}
		}

		if (rack.getMasterHost() != null) {
			this.masterHost = rack.getMasterHost();
			if (this.masterHost.getJobTracker() != null)
				this.jobTracker = this.masterHost.getJobTracker();
		}
	}

	public void addMasterHostInfo(MasterHostInfo host) {
		RackInfo rack = (RackInfo) this.racks.get(host.getRackName());
		rack.setMasterHost(host);

		this.masterHost = host;
		if (this.masterHost.getJobTracker() != null)
			this.jobTracker = this.masterHost.getJobTracker();
	}

	public void addSlaveHostInfo(SlaveHostInfo host) {
		RackInfo rack = (RackInfo) this.racks.get(host.getRackName());
		rack.addSlaveHost(host);

		this.slaveHosts.put(host.getName(), host);
		TaskTrackerInfo taskTracker = host.getTaskTracker();
		if (taskTracker != null)
			this.taskTrackers.put(taskTracker.getName(), taskTracker);
	}

	public void addJobTrackerInfo(JobTrackerInfo tracker) {
		this.masterHost.setJobTracker(tracker);
		this.jobTracker = tracker;
	}

	public void addTaskTrackerInfo(TaskTrackerInfo taskTracker) {
		SlaveHostInfo slave = (SlaveHostInfo) this.slaveHosts.get(taskTracker
				.getHostName());
		slave.setTaskTracker(taskTracker);

		this.taskTrackers.put(taskTracker.getName(), taskTracker);
	}

	public RackInfo addFindRackInfo(String rackName) {
		if (this.racks.containsKey(rackName)) {
			return (RackInfo) this.racks.get(rackName);
		}

		RackInfo rack = new RackInfo();
		rack.setName(rackName);
		this.racks.put(rackName, rack);
		return rack;
	}

	public MasterHostInfo addFindMasterHostInfo(String fullHostName) {
		String rackName = null;
		String hostName = null;
		String[] pieces = fullHostName.split("/");

		if (pieces.length == 3) {
			rackName = pieces[1];
			hostName = pieces[2];
		} else if (pieces.length > 3) {
			rackName = fullHostName.substring(0, fullHostName.lastIndexOf(47));
			hostName = pieces[(pieces.length - 1)];
		} else if ((this.masterHost != null)
				&& (fullHostName.equals(this.masterHost.getName()))) {
			rackName = this.masterHost.getRackName();
			hostName = this.masterHost.getName();
		} else {
			return null;
		}

		MasterHostInfo host = null;
		RackInfo rack = addFindRackInfo(rackName);
		if ((rack.getMasterHost() != null)
				&& (hostName.equals(rack.getMasterHost().getName()))) {
			host = rack.getMasterHost();
		} else {
			host = new MasterHostInfo();
			host.setName(hostName);
			host.setRackName(rackName);
		}

		addMasterHostInfo(host);
		return host;
	}

	public SlaveHostInfo addFindSlaveHostInfo(String fullHostName) {
		String rackName = null;
		String hostName = null;
		String[] pieces = fullHostName.split("/");

		if (pieces.length == 3) {
			rackName = pieces[1];
			hostName = pieces[2];
		} else if (pieces.length > 3) {
			rackName = fullHostName.substring(0, fullHostName.lastIndexOf(47));
			hostName = pieces[(pieces.length - 1)];
		} else if ((this.slaveHosts.containsKey(fullHostName))
				&& (fullHostName.equals(((SlaveHostInfo) this.slaveHosts
						.get(fullHostName)).getName()))) {
			rackName = ((SlaveHostInfo) this.slaveHosts.get(fullHostName))
					.getRackName();
			hostName = ((SlaveHostInfo) this.slaveHosts.get(fullHostName))
					.getName();
		} else {
			return null;
		}

		SlaveHostInfo host = null;
		RackInfo rack = addFindRackInfo(rackName);
		if (rack.getSlaveHost(hostName) != null) {
			host = rack.getSlaveHost(hostName);
		} else {
			host = new SlaveHostInfo();
			host.setName(hostName);
			host.setRackName(rackName);
		}

		addSlaveHostInfo(host);
		return host;
	}

	public JobTrackerInfo addFindJobTrackerInfo(String trackerName,
			String fullHostName) {
		JobTrackerInfo tracker = null;
		MasterHostInfo masterHost = addFindMasterHostInfo(fullHostName);
		if (masterHost == null)
			return null;
		if ((masterHost.getJobTracker() != null)
				&& (trackerName.equals(masterHost.getJobTracker().getName()))) {
			tracker = masterHost.getJobTracker();
		} else {
			tracker = new JobTrackerInfo();
			tracker.setName(trackerName);
			tracker.setHostName(masterHost.getName());
		}

		addJobTrackerInfo(tracker);
		return tracker;
	}

	public TaskTrackerInfo addFindTaskTrackerInfo(String trackerName,
			String fullHostName) {
		TaskTrackerInfo tracker = null;
		SlaveHostInfo host = addFindSlaveHostInfo(fullHostName);
		if (host == null)
			return null;
		if ((host.getTaskTracker() != null)
				&& (trackerName.equals(host.getTaskTracker().getName()))) {
			tracker = host.getTaskTracker();
		} else {
			tracker = new TaskTrackerInfo();
			tracker.setName(trackerName);
			tracker.setHostName(host.getName());
		}

		addTaskTrackerInfo(tracker);
		return tracker;
	}

	public Collection<RackInfo> getAllRackInfos() {
		return this.racks.values();
	}

	public Collection<SlaveHostInfo> getAllSlaveHostInfos() {
		return this.slaveHosts.values();
	}

	public Collection<TaskTrackerInfo> getAllTaskTrackersInfos() {
		return this.taskTrackers.values();
	}

	public RackInfo getRackInfo(String name) {
		return (RackInfo) this.racks.get(name);
	}

	public MasterHostInfo getMasterHostInfo() {
		return this.masterHost;
	}

	public HostInfo getSlaveHostInfo(String name) {
		return (HostInfo) this.slaveHosts.get(name);
	}

	public JobTrackerInfo getJobTrackerInfo() {
		return this.jobTracker;
	}

	public TaskTrackerInfo getTaskTrackerInfo(String name) {
		return (TaskTrackerInfo) this.taskTrackers.get(name);
	}

	public String getClusterName() {
		return this.name;
	}

	public int getNumberOfHosts() {
		return this.taskTrackers.size() + (this.masterHost != null ? 1 : 0);
	}

	public int getAvgMapSlotsPerHost() {
		return Math.round(getTotalMapSlots() / this.taskTrackers.size());
	}

	public int getAvgReduceSlotsPerHost() {
		return Math.round(getTotalReduceSlots() / this.taskTrackers.size());
	}

	public int getTotalMapSlots() {
		int numSlots = 0;
		for (TaskTrackerInfo taskTracker : this.taskTrackers.values()) {
			numSlots += taskTracker.getNumMapSlots();
		}
		return numSlots;
	}

	public int getTotalReduceSlots() {
		int numSlots = 0;
		for (TaskTrackerInfo taskTracker : this.taskTrackers.values()) {
			numSlots += taskTracker.getNumReduceSlots();
		}
		return numSlots;
	}

	public long getMaxMapTaskMemory() {
		long maxMem = 0L;
		for (TaskTrackerInfo taskTracker : this.taskTrackers.values()) {
			maxMem += taskTracker.getMaxMapTaskMemory();
		}

		return Math.round(maxMem / this.taskTrackers.size());
	}

	public long getMaxReduceTaskMemory() {
		long maxMem = 0L;
		for (TaskTrackerInfo taskTracker : this.taskTrackers.values()) {
			maxMem += taskTracker.getMaxReduceTaskMemory();
		}

		return Math.round(maxMem / this.taskTrackers.size());
	}

	public void setClusterName(String name) {
		this.name = name;
	}

	public void mergeCluster(ClusterConfiguration other) {
		if (this.name == null) {
			setClusterName(other.getClusterName());
		}

		for (RackInfo rack : other.getAllRackInfos()) {
			if (this.racks.containsKey(rack.getName())) {
				if ((this.masterHost == null) && (rack.getMasterHost() != null)) {
					addMasterHostInfo(new MasterHostInfo(rack.getMasterHost()));
				}
				for (SlaveHostInfo slave : rack.getSlaveHosts())
					if (!this.slaveHosts.containsKey(slave.getName()))
						addSlaveHostInfo(new SlaveHostInfo(slave));
			} else {
				addRackInfo(new RackInfo(rack));
			}
		}
	}

	public void printClusterConfiguration(PrintStream out) {
		if (this.name != null) {
			out.println("Cluster: " + this.name);
			out.println();
		}

		for (RackInfo rack : this.racks.values()) {
			out.println("Rack: " + rack.getName());
			if (rack.getMasterHost() != null) {
				out.println("\tMaster Host: " + rack.getMasterHost().getName());
			}

			for (SlaveHostInfo host : rack.getSlaveHosts()) {
				out.println("\tSlave Host: " + host.getName());
			}

		}

		out.println();
		out.println("Job Trackers:");
		out.println("Name\tHost\tPort");
		if (this.jobTracker != null) {
			out.print(this.jobTracker.getName());
			out.print("\t");
			out.print(this.jobTracker.getHostName());
			out.print("\t");
			out.println(this.jobTracker.getPort());
		}

		out.println();
		out.println("Task Trackers:");
		out.println("Name\tHost\tPort\tMap_Slots\tReduce_Slots\tMax_Map_Mem(MB)\tMax_Reduce_Mem(MB)");

		for (TaskTrackerInfo taskTracker : this.taskTrackers.values()) {
			out.print(taskTracker.getName());
			out.print("\t");
			out.print(taskTracker.getHostName());
			out.print("\t");
			out.print(taskTracker.getPort());
			out.print("\t");
			out.print(taskTracker.getNumMapSlots());
			out.print("\t");
			out.print(taskTracker.getNumReduceSlots());
			out.print("\t");
			out.print(taskTracker.getMaxMapTaskMemory() >> 20);
			out.print("\t");
			out.println(taskTracker.getMaxReduceTaskMemory() >> 20);
		}
	}

	public static ClusterConfiguration createClusterConfiguration(String name,
			int numRacks, int numHostsPerRack, int numMapSlots,
			int numReduceSlots, long maxMapTaskMemory, long maxRedTaskMemory) {
		NumberFormat nf = NumberFormat.getIntegerInstance();
		nf.setMinimumIntegerDigits(3);

		ClusterConfiguration cluster = new ClusterConfiguration();
		cluster.setClusterName(name);

		for (int rackId = 1; rackId <= numRacks; rackId++) {
			RackInfo rack = new RackInfo(rackId, "rack_" + nf.format(rackId));
			cluster.addRackInfo(rack);

			if (rackId == 1) {
				MasterHostInfo host = new MasterHostInfo(0L, "master_host", "",
						rack.getName());

				JobTrackerInfo tracker = new JobTrackerInfo(0L, "job_tracker_"
						+ host.getName(), host.getName(), 50060);

				cluster.addMasterHostInfo(host);
				cluster.addJobTrackerInfo(tracker);
			}

			for (int hostId = 1; hostId <= numHostsPerRack; hostId++) {
				int id = (rackId - 1) * numHostsPerRack + hostId;

				SlaveHostInfo host = new SlaveHostInfo(id, "rack_"
						+ nf.format(rackId) + "_host_" + nf.format(hostId), "",
						rack.getName());

				TaskTrackerInfo tracker = new TaskTrackerInfo(id,
						"task_tracker_" + host.getName(), host.getName(),
						50060, numMapSlots, numReduceSlots, maxMapTaskMemory,
						maxRedTaskMemory);

				cluster.addSlaveHostInfo(host);
				cluster.addTaskTrackerInfo(tracker);
			}
		}

		return cluster;
	}
}