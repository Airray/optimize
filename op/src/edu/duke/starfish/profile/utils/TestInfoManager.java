package edu.duke.starfish.profile.utils;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.IMRInfoManager;
import edu.duke.starfish.profile.profileinfo.execution.DataLocality;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRCleanupAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRSetupAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRCleanupInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRMapInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRReduceInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRSetupInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.metrics.DataTransfer;
import edu.duke.starfish.profile.profileinfo.metrics.Metric;
import edu.duke.starfish.profile.profileinfo.metrics.MetricType;
import edu.duke.starfish.profile.profileinfo.setup.HostInfo;
import edu.duke.starfish.profile.profileinfo.setup.JobTrackerInfo;
import edu.duke.starfish.profile.profileinfo.setup.MasterHostInfo;
import edu.duke.starfish.profile.profileinfo.setup.RackInfo;
import edu.duke.starfish.profile.profileinfo.setup.SlaveHostInfo;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;

public class TestInfoManager implements IMRInfoManager {
	private ClusterConfiguration cluster;
	private MRJobInfo jobInfo;
	private Random r;
	private NumberFormat nf;

	public TestInfoManager(int numRacks, int numHostsPerRack, int numMapSlots,
			int numReduceSlots, int numMappers, int numReducers) {
		this.r = new Random();
		this.nf = NumberFormat.getIntegerInstance();

		this.cluster = generateClusterConfiguration(numRacks, numHostsPerRack,
				numMapSlots, numReduceSlots);

		this.jobInfo = generateTestMRJob(1, this.cluster, numMappers,
				numReducers);
		this.jobInfo.addDataTransfers(generateDataTransferInJob(this.jobInfo));
	}

	public List<MRJobInfo> getAllMRJobInfos() {
		List allJobs = new ArrayList(1);
		allJobs.add(this.jobInfo);
		return allJobs;
	}

	public List<MRJobInfo> getAllMRJobInfos(Date start, Date end) {
		if ((this.jobInfo.getStartTime().after(start))
				&& (this.jobInfo.getEndTime().before(end))) {
			return getAllMRJobInfos();
		}
		return new ArrayList(0);
	}

	public MRJobInfo getMRJobInfo(String execId) {
		if (this.jobInfo.getExecId().equalsIgnoreCase(execId)) {
			return this.jobInfo;
		}
		return null;
	}

	public ClusterConfiguration getClusterConfiguration(String mrJobId) {
		if (mrJobId.equals(this.jobInfo.getExecId())) {
			return this.cluster;
		}
		return null;
	}

	public boolean loadDataTransfersForMRJob(MRJobInfo mrJob) {
		return mrJob == this.jobInfo;
	}

	public boolean loadTaskDetailsForMRJob(MRJobInfo mrJob) {
		return mrJob == this.jobInfo;
	}

	public boolean loadProfilesForMRJob(MRJobInfo mrJob) {
		return mrJob == this.jobInfo;
	}

	public Configuration getHadoopConfiguration(String mrJobId) {
		return new Configuration(false);
	}

	public MRJobProfile getMRJobProfile(String mrJobId) {
		if (mrJobId.equals(this.jobInfo.getExecId())) {
			return this.jobInfo.getProfile();
		}
		return null;
	}

	public List<Metric> getHostMetrics(MetricType type, HostInfo host, Date start, Date end)
  {
    switch (1.$SwitchMap$edu$duke$starfish$profile$profileinfo$metrics$MetricType[type.ordinal()]) {
    case 1:
      return generateMetrics(start, end, 0.3D, 0.8D);
    case 2:
      return generateMetrics(start, end, 500.0D, 1200.0D);
    case 3:
      return generateMetrics(start, end, 100.0D, 500.0D);
    case 4:
      return generateMetrics(start, end, 100.0D, 500.0D);
    case 5:
      return generateMetrics(start, end, 100.0D, 500.0D);
    case 6:
      return generateMetrics(start, end, 100.0D, 500.0D);
    }

    return null;
  }

	private ClusterConfiguration generateClusterConfiguration(int numRacks,
			int numHostsPerRack, int numMapSlots, int numReduceSlots) {
		this.nf.setMinimumIntegerDigits(3);

		ClusterConfiguration cluster = new ClusterConfiguration();

		for (int rackId = 1; rackId <= numRacks; rackId++) {
			RackInfo rack = new RackInfo(rackId, "rack_"
					+ this.nf.format(rackId));
			cluster.addRackInfo(rack);

			if (rackId == 1) {
				MasterHostInfo host = new MasterHostInfo(0L,
						"hadoop.cs.duke.edu", "127.0.0.1:10"
								+ this.nf.format(0L), rack.getName());

				JobTrackerInfo tracker = new JobTrackerInfo(0L, "job_tracker_"
						+ host.getName(), host.getName(), 50060);

				cluster.addMasterHostInfo(host);
				cluster.addJobTrackerInfo(tracker);
			}

			for (int hostId = 1; hostId <= numHostsPerRack; hostId++) {
				int id = (rackId - 1) * numHostsPerRack + hostId;

				SlaveHostInfo host = new SlaveHostInfo(id, "rack"
						+ this.nf.format(rackId) + ".hadoop"
						+ this.nf.format(hostId) + ".cs.duke.edu",
						"127.0.0.1:10" + this.nf.format(id), rack.getName());

				TaskTrackerInfo tracker = new TaskTrackerInfo(id,
						"task_tracker_" + host.getName(), host.getName(),
						50060, numMapSlots, numReduceSlots, 209715200L,
						209715200L);

				cluster.addSlaveHostInfo(host);
				cluster.addTaskTrackerInfo(tracker);
			}
		}

		return cluster;
	}

	private MRJobInfo generateTestMRJob(int jobId, ClusterConfiguration cluster, int numMappers, int numReducers)
  {
    this.r.setSeed(jobId);
    this.nf.setGroupingUsed(false);
    this.nf.setMinimumIntegerDigits(4);

    String id = this.nf.format(jobId);
    Date startTime = new Date(1271077200035L);
    MRJobInfo job = new MRJobInfo(jobId, "job_" + id, startTime, null, MRExecutionStatus.SUCCESS, null, "test_job_" + id, "user");

    ArrayList hosts = new ArrayList(cluster.getAllSlaveHostInfos());

    Date setupEndTime = new Date(()(this.r.nextDouble() * 5.0D * 1000.0D + startTime.getTime()));

    job.addSetupTaskInfo(createTestSetupInfo(id, startTime, setupEndTime, (SlaveHostInfo)hosts.get(0)));

    generateMapReduceTasks(job, id, numMappers, hosts, true);

    generateMapReduceTasks(job, id, numReducers, hosts, false);

    Date cleanupStartTime = numReducers == 0 ? getLastMapEndTime(job.getMapTasks()) : getLastReduceEndTime(job.getReduceTasks());

    Date cleanupEndTime = new Date(()(this.r.nextDouble() * 5.0D * 1000.0D + cleanupStartTime.getTime()));

    job.addCleanupTaskInfo(createTestCleanupInfo(id, cleanupStartTime, cleanupEndTime, (SlaveHostInfo)hosts.get(0)));

    job.setEndTime(cleanupEndTime);

    return job;
  }

	private List<DataTransfer> generateDataTransferInJob(MRJobInfo job) {
		int numRed = job.getReduceTasks().size();
		List dataTransfers = new ArrayList(numRed);

		if (numRed == 0) {
			return dataTransfers;
		}

		long data = 0L;
		for (MRMapInfo map : job.getMapTasks()) {
			MRMapAttemptInfo mapAttempt = (MRMapAttemptInfo) map.getAttempts()
					.get(0);

			for (int i = 0; i < numRed; i++) {
				MRReduceInfo reduce = (MRReduceInfo) job.getReduceTasks()
						.get(i);
				MRReduceAttemptInfo redAttempt = (MRReduceAttemptInfo) reduce
						.getAttempts().get(0);

				if (this.r.nextDouble() < 0.75D) {
					data = Math.abs(this.r.nextLong() % 100000L);
					dataTransfers.add(new DataTransfer(mapAttempt, redAttempt,
							data, data));
				}
			}

		}

		return dataTransfers;
	}

	private MRSetupInfo createTestSetupInfo(String jobId, Date startTime,
			Date endTime, SlaveHostInfo host) {
		MRSetupInfo setup = new MRSetupInfo(0L, "task_" + jobId + "_setup",
				startTime, endTime, MRExecutionStatus.SUCCESS, null);

		MRSetupAttemptInfo setupAttempt = new MRSetupAttemptInfo(0L,
				setup.getExecId() + "_1", startTime, endTime,
				MRExecutionStatus.SUCCESS, null, host.getTaskTracker());

		setup.addAttempt(setupAttempt);

		return setup;
	}

	private MRCleanupInfo createTestCleanupInfo(String jobId, Date startTime,
			Date endTime, SlaveHostInfo host) {
		MRCleanupInfo cleanup = new MRCleanupInfo(0L, "task_" + jobId
				+ "_cleanup", startTime, endTime, MRExecutionStatus.SUCCESS,
				null);

		MRCleanupAttemptInfo cleanupAttempt = new MRCleanupAttemptInfo(0L,
				cleanup.getExecId() + "_1", startTime, endTime,
				MRExecutionStatus.SUCCESS, null, host.getTaskTracker());

		cleanup.addAttempt(cleanupAttempt);

		return cleanup;
	}

	private MRMapInfo createTestMapInfo(String jobId, long mapId,
			Date startTime, Date endTime, SlaveHostInfo host) {
		List hosts = new ArrayList(1);
		hosts.add(host);

		MRMapInfo map = new MRMapInfo(mapId, "task_" + jobId + "_m_"
				+ this.nf.format(mapId), startTime, endTime,
				MRExecutionStatus.SUCCESS, null, hosts);

		MRMapAttemptInfo mapAttempt = new MRMapAttemptInfo(mapId,
				map.getExecId() + "_1", startTime, endTime,
				MRExecutionStatus.SUCCESS, null, host.getTaskTracker(),
				DataLocality.DATA_LOCAL);

		map.addAttempt(mapAttempt);

		MRTaskProfile profile = mapAttempt.getProfile();
		profile.addCounter(MRCounter.COMBINE_INPUT_RECORDS,
				Long.valueOf(Math.abs(this.r.nextLong() % 100000L)));

		profile.addCounter(MRCounter.COMBINE_OUTPUT_RECORDS,
				Long.valueOf(Math.abs(this.r.nextLong() % 100000L)));

		profile.addCounter(MRCounter.MAP_INPUT_BYTES,
				Long.valueOf(Math.abs(this.r.nextLong() % 100000L)));

		profile.addCounter(MRCounter.MAP_INPUT_RECORDS,
				Long.valueOf(Math.abs(this.r.nextLong() % 100000L)));

		profile.addCounter(MRCounter.MAP_OUTPUT_BYTES,
				Long.valueOf(Math.abs(this.r.nextLong() % 100000L)));

		profile.addCounter(MRCounter.MAP_OUTPUT_RECORDS,
				Long.valueOf(Math.abs(this.r.nextLong() % 100000L)));

		profile.addCounter(MRCounter.SPILLED_RECORDS,
				Long.valueOf(Math.abs(this.r.nextLong() % 100000L)));

		profile.addCounter(MRCounter.HDFS_BYTES_READ,
				Long.valueOf(Math.abs(this.r.nextLong() % 100000L)));

		return map;
	}

	private MRReduceInfo createTestReduceInfo(String id, long redId, Date startTime, Date endTime, Date lastMapEndTime, SlaveHostInfo host)
  {
    long minEndShuffle = Math.max(startTime.getTime(), lastMapEndTime.getTime());

    Date shuffle = new Date(()(0.4D * (endTime.getTime() - minEndShuffle) + minEndShuffle));

    Date sort = new Date(()(0.7D * (endTime.getTime() - minEndShuffle) + minEndShuffle));

    MRReduceInfo reducer = new MRReduceInfo(redId, "task_" + id + "_r_" + this.nf.format(redId), startTime, endTime, MRExecutionStatus.SUCCESS, null);

    MRReduceAttemptInfo reducerAttempt = new MRReduceAttemptInfo(redId, reducer.getExecId() + "_1", startTime, endTime, MRExecutionStatus.SUCCESS, null, host.getTaskTracker(), shuffle, sort);

    reducer.addAttempt(reducerAttempt);

    MRTaskProfile profile = reducerAttempt.getProfile();
    profile.addCounter(MRCounter.REDUCE_INPUT_GROUPS, Long.valueOf(Math.abs(this.r.nextLong() % 1000L)));

    profile.addCounter(MRCounter.REDUCE_SHUFFLE_BYTES, Long.valueOf(Math.abs(this.r.nextLong() % 100000L)));

    profile.addCounter(MRCounter.REDUCE_INPUT_RECORDS, Long.valueOf(Math.abs(this.r.nextLong() % 100000L)));

    profile.addCounter(MRCounter.REDUCE_OUTPUT_RECORDS, Long.valueOf(Math.abs(this.r.nextLong() % 100000L)));

    profile.addCounter(MRCounter.HDFS_BYTES_WRITTEN, Long.valueOf(Math.abs(this.r.nextLong() % 100000L)));

    return reducer;
  }

	private void generateMapReduceTasks(MRJobInfo job, String id, int numTasks, Collection<SlaveHostInfo> hosts, boolean maps)
  {
    int numTrackers = hosts.size();
    if (numTrackers == 0) {
      return;
    }

    TaskTrackerInfo firstTracker = ((SlaveHostInfo)hosts.iterator().next()).getTaskTracker();
    int numSlotsPerTracker = maps ? firstTracker.getNumMapSlots() : firstTracker.getNumReduceSlots();

    Date lastMapEndTime = getLastMapEndTime(job.getMapTasks());

    int totalNumSlots = numTrackers * numSlotsPerTracker;
    int numCompleteWaves = numTasks / totalNumSlots;
    int numSlotsExtraTasks = numTasks % totalNumSlots;

    long taskId = 1L;
    Iterator iterator = hosts.iterator();
    SlaveHostInfo host = (SlaveHostInfo)iterator.next();

    int numSlots = Math.min(totalNumSlots, numTasks);
    for (int slot = 0; slot < numSlots; slot++)
    {
      int numWaves = numCompleteWaves + (slot < numSlotsExtraTasks ? 1 : 0);

      Date taskStart = null;
      Date prevEnd = new Date(job.getStartTime().getTime() + 5000L);

      for (int j = 0; j < numWaves; j++)
      {
        taskStart = new Date(()(this.r.nextDouble() * 10.0D * 1000.0D + prevEnd.getTime()));

        prevEnd = new Date(()(this.r.nextDouble() * 10.0D * 60.0D * 1000.0D + taskStart.getTime()));

        if (maps) {
          job.addMapTaskInfo(createTestMapInfo(id, taskId, taskStart, prevEnd, host));
        }
        else {
          if (prevEnd.before(lastMapEndTime))
          {
            prevEnd = new Date(()(this.r.nextDouble() * 10.0D * 60.0D * 1000.0D + lastMapEndTime.getTime()));
          }

          job.addReduceTaskInfo(createTestReduceInfo(id, taskId, taskStart, prevEnd, lastMapEndTime, host));
        }

        taskId += 1L;
      }

      if ((slot + 1) % numSlotsPerTracker == 0)
        host = iterator.hasNext() ? (SlaveHostInfo)iterator.next() : null;
    }
  }

	private List<Metric> generateMetrics(Date start, Date end, double low, double high)
  {
    List metrics = new ArrayList();

    long interval = ()(this.r.nextDouble() * 10000.0D + 10000.0D);

    long time = start.getTime();
    while (time <= end.getTime()) {
      metrics.add(new Metric(new Date(time), this.r.nextDouble() * (high - low) + low));

      time += interval;
    }

    return metrics;
  }

	private Date getLastMapEndTime(List<MRMapInfo> maps) {
		Date maxEndTime = new Date(0L);

		for (MRMapInfo map : maps) {
			for (MRMapAttemptInfo mapAttempt : map.getAttempts()) {
				if (mapAttempt.getEndTime().after(maxEndTime)) {
					maxEndTime = mapAttempt.getEndTime();
				}
			}
		}

		return maxEndTime;
	}

	private Date getLastReduceEndTime(List<MRReduceInfo> reducers) {
		Date maxEndTime = new Date(0L);

		for (MRReduceInfo reduce : reducers) {
			for (MRReduceAttemptInfo reduceAttempt : reduce.getAttempts()) {
				if (reduceAttempt.getEndTime().after(maxEndTime)) {
					maxEndTime = reduceAttempt.getEndTime();
				}
			}
		}

		return maxEndTime;
	}
}