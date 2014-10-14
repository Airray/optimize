package edu.duke.starfish.profile.utils;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.setup.HostInfo;
import edu.duke.starfish.profile.profileinfo.setup.JobTrackerInfo;
import edu.duke.starfish.profile.profileinfo.setup.MasterHostInfo;
import edu.duke.starfish.profile.profileinfo.setup.RackInfo;
import edu.duke.starfish.profile.profileinfo.setup.SlaveHostInfo;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class XMLClusterParser extends XMLBaseParser<ClusterConfiguration> {
	private static final String CLUSTER = "cluster";
	private static final String RACK = "rack";
	private static final String MASTER_HOST = "master_host";
	private static final String SLAVE_HOST = "slave_host";
	private static final String JOB_TRACKER = "job_tracker";
	private static final String TASK_TRACKER = "task_tracker";
	private static final String SPECS = "specs";
	private static final String NAME = "name";
	private static final String IP_ADDRESS = "ip";
	private static final String PORT = "port";
	private static final String MAP_SLOTS = "map_slots";
	private static final String RED_SLOTS = "reduce_slots";
	private static final String MAX_SLOT_MEMORY = "max_slot_memory";
	private static final String MAX_MAP_TASK_MEMORY = "max_map_task_memory";
	private static final String MAX_RED_TASK_MEMORY = "max_red_task_memory";
	private static final String NUM_RACKS = "num_racks";
	private static final String HOSTS_PER_RACK = "hosts_per_rack";
	private static final String MAP_SLOTS_PER_HOST = "map_slots_per_host";
	private static final String RED_SLOTS_PER_HOST = "reduce_slots_per_host";

	protected ClusterConfiguration importXML(Document doc) {
		Element root = doc.getDocumentElement();
		if (!"cluster".equals(root.getTagName())) {
			throw new RuntimeException(
					"ERROR: Bad XML File: top-level element not <cluster>");
		}

		ClusterConfiguration cluster = null;
		String name = root.getAttribute("name");

		NodeList specs = root.getElementsByTagName("specs");
		if (specs.getLength() != 0) {
			cluster = loadClusterFromSpecs(name, (Element) specs.item(0));
		} else {
			cluster = new ClusterConfiguration();
			cluster.setClusterName(name);
			NodeList racks = root.getElementsByTagName("rack");
			for (int i = 0; i < racks.getLength(); i++) {
				if ((racks.item(i) instanceof Element)) {
					Element rack = (Element) racks.item(i);
					loadRack(cluster, rack);
				}
			}
		}

		return cluster;
	}

	protected void exportXML(ClusterConfiguration cluster, Document doc) {
		Element clusterElem = doc.createElement("cluster");
		doc.appendChild(clusterElem);

		if (cluster.getClusterName() != null) {
			clusterElem.setAttribute("name", cluster.getClusterName());
		}

		for (RackInfo rack : cluster.getAllRackInfos())
			clusterElem.appendChild(buildRackElement(rack, doc));
	}

	private Element buildRackElement(RackInfo rack, Document doc) {
		Element rackElem = doc.createElement("rack");
		rackElem.setAttribute("name", rack.getName());

		MasterHostInfo masterHost = rack.getMasterHost();
		if (masterHost != null) {
			rackElem.appendChild(buildMasterHostElement(masterHost, doc));
		}

		for (HostInfo host : rack.getSlaveHosts()) {
			rackElem.appendChild(buildSlaveHostElement((SlaveHostInfo) host,
					doc));
		}

		return rackElem;
	}

	private Element buildMasterHostElement(MasterHostInfo host, Document doc) {
		Element hostElem = doc.createElement("master_host");
		hostElem.setAttribute("name", host.getName());
		if (host.getIpAddress() != null) {
			hostElem.setAttribute("ip", host.getIpAddress());
		}

		JobTrackerInfo jobTracker = host.getJobTracker();
		if (jobTracker != null) {
			Element jobTrackerElem = doc.createElement("job_tracker");
			jobTrackerElem.setAttribute("name", jobTracker.getName());
			jobTrackerElem.setAttribute("port",
					Integer.toString(jobTracker.getPort()));

			hostElem.appendChild(jobTrackerElem);
		}

		return hostElem;
	}

	private Element buildSlaveHostElement(SlaveHostInfo host, Document doc) {
		Element hostElem = doc.createElement("slave_host");
		hostElem.setAttribute("name", host.getName());
		if (host.getIpAddress() != null) {
			hostElem.setAttribute("ip", host.getIpAddress());
		}

		TaskTrackerInfo taskTracker = host.getTaskTracker();
		if (taskTracker != null) {
			Element taskTrackerElem = doc.createElement("task_tracker");

			taskTrackerElem.setAttribute("name", taskTracker.getName());
			taskTrackerElem.setAttribute("port",
					Integer.toString(taskTracker.getPort()));

			taskTrackerElem.setAttribute("map_slots",
					Integer.toString(taskTracker.getNumMapSlots()));

			taskTrackerElem.setAttribute("reduce_slots",
					Integer.toString(taskTracker.getNumReduceSlots()));

			taskTrackerElem.setAttribute("max_map_task_memory",
					Long.toString(taskTracker.getMaxMapTaskMemory() >> 20));

			taskTrackerElem.setAttribute("max_red_task_memory",
					Long.toString(taskTracker.getMaxReduceTaskMemory() >> 20));

			hostElem.appendChild(taskTrackerElem);
		}

		return hostElem;
	}

	private ClusterConfiguration loadClusterFromSpecs(String clusterName,
			Element specsElem) {
		int numRacks = 1;
		String numRacksStr = specsElem.getAttribute("num_racks");
		if ((numRacksStr != null) && (!numRacksStr.equals(""))) {
			numRacks = Integer.parseInt(numRacksStr);
		}

		int numHostsPerRack = Integer.parseInt(specsElem
				.getAttribute("hosts_per_rack"));

		int numMapSlots = Integer.parseInt(specsElem
				.getAttribute("map_slots_per_host"));

		int numRedSlots = Integer.parseInt(specsElem
				.getAttribute("reduce_slots_per_host"));

		String maxSlotMemory = specsElem.getAttribute("max_slot_memory");
		long maxMapTaskMem;
		long maxMapTaskMem;
		long maxRedTaskMem;
		if ((maxSlotMemory != null) && (!maxSlotMemory.equals(""))) {
			long maxRedTaskMem;
			maxMapTaskMem = maxRedTaskMem = Long.parseLong(maxSlotMemory) << 20;
		} else {
			maxMapTaskMem = Long.parseLong(specsElem
					.getAttribute("max_map_task_memory")) << 20;

			maxRedTaskMem = Long.parseLong(specsElem
					.getAttribute("max_red_task_memory")) << 20;
		}

		return ClusterConfiguration.createClusterConfiguration(clusterName,
				numRacks, numHostsPerRack, numMapSlots, numRedSlots,
				maxMapTaskMem, maxRedTaskMem);
	}

	private void loadRack(ClusterConfiguration cluster, Element rack) {
		RackInfo rackInfo = new RackInfo();
		rackInfo.setName(rack.getAttribute("name"));

		NodeList master_hosts = rack.getElementsByTagName("master_host");
		for (int i = 0; i < master_hosts.getLength(); i++) {
			if ((master_hosts.item(i) instanceof Element)) {
				Element host = (Element) master_hosts.item(i);
				loadMasterHost(rackInfo, host);
			}

		}

		NodeList slave_hosts = rack.getElementsByTagName("slave_host");
		for (int i = 0; i < slave_hosts.getLength(); i++) {
			if ((slave_hosts.item(i) instanceof Element)) {
				Element host = (Element) slave_hosts.item(i);
				loadSlaveHost(rackInfo, host);
			}

		}

		cluster.addRackInfo(rackInfo);
	}

	private void loadMasterHost(RackInfo rack, Element host) {
		MasterHostInfo hostInfo = new MasterHostInfo();
		hostInfo.setName(host.getAttribute("name"));
		if ((host.getAttribute("ip") != null)
				&& (!host.getAttribute("ip").equals(""))) {
			hostInfo.setIpAddress(host.getAttribute("ip"));
		}

		NodeList trackers = host.getElementsByTagName("job_tracker");
		for (int i = 0; i < trackers.getLength(); i++) {
			if ((trackers.item(i) instanceof Element)) {
				Element tracker = (Element) trackers.item(i);
				loadJobTracker(hostInfo, tracker);
			}

		}

		rack.setMasterHost(hostInfo);
	}

	private void loadSlaveHost(RackInfo rack, Element host) {
		SlaveHostInfo hostInfo = new SlaveHostInfo();
		hostInfo.setName(host.getAttribute("name"));
		if ((host.getAttribute("ip") != null)
				&& (!host.getAttribute("ip").equals(""))) {
			hostInfo.setIpAddress(host.getAttribute("ip"));
		}

		NodeList trackers = host.getElementsByTagName("task_tracker");
		for (int i = 0; i < trackers.getLength(); i++) {
			if ((trackers.item(i) instanceof Element)) {
				Element tracker = (Element) trackers.item(i);
				loadTaskTracker(hostInfo, tracker);
			}

		}

		rack.addSlaveHost(hostInfo);
	}

	private void loadJobTracker(MasterHostInfo host, Element jobTracker) {
		JobTrackerInfo jobTrackerInfo = new JobTrackerInfo();

		jobTrackerInfo.setName(jobTracker.getAttribute("name"));
		if ((jobTracker.getAttribute("port") != null)
				&& (!jobTracker.getAttribute("port").equals(""))) {
			jobTrackerInfo.setPort(Integer.parseInt(jobTracker
					.getAttribute("port")));
		}

		host.setJobTracker(jobTrackerInfo);
	}

	private void loadTaskTracker(SlaveHostInfo host, Element taskTracker) {
		TaskTrackerInfo taskTrackerInfo = new TaskTrackerInfo();
		taskTrackerInfo.setName(taskTracker.getAttribute("name"));

		String port = taskTracker.getAttribute("port");
		if ((port != null) && (!port.equals(""))) {
			taskTrackerInfo.setPort(Integer.parseInt(port));
		}
		taskTrackerInfo.setNumMapSlots(Integer.parseInt(taskTracker
				.getAttribute("map_slots")));

		taskTrackerInfo.setNumReduceSlots(Integer.parseInt(taskTracker
				.getAttribute("reduce_slots")));

		String maxMem = taskTracker.getAttribute("max_slot_memory");
		if ((maxMem != null) && (!maxMem.equals(""))) {
			long mem = Long.parseLong(maxMem) << 20;
			taskTrackerInfo.setMaxMapTaskMemory(mem);
			taskTrackerInfo.setMaxReduceTaskMemory(mem);
		} else {
			maxMem = taskTracker.getAttribute("max_map_task_memory");
			if ((maxMem != null) && (!maxMem.equals(""))) {
				taskTrackerInfo
						.setMaxMapTaskMemory(Long.parseLong(maxMem) << 20);
			}

			maxMem = taskTracker.getAttribute("max_red_task_memory");
			if ((maxMem != null) && (!maxMem.equals(""))) {
				taskTrackerInfo
						.setMaxReduceTaskMemory(Long.parseLong(maxMem) << 20);
			}

		}

		host.setTaskTracker(taskTrackerInfo);
	}
}