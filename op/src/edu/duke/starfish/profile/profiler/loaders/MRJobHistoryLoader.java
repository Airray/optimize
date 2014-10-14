package edu.duke.starfish.profile.profiler.loaders;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.DataLocality;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRCleanupAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRSetupAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRCleanupInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRMapInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRReduceInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRSetupInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRTaskInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.setup.JobTrackerInfo;
import edu.duke.starfish.profile.profileinfo.setup.SlaveHostInfo;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;
import edu.duke.starfish.profile.utils.ProfileUtils;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.DefaultJobHistoryParser;
import org.apache.hadoop.mapred.JobHistory.JobInfo;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.apache.hadoop.mapred.JobHistory.Task;
import org.apache.hadoop.mapred.JobHistory.TaskAttempt;
import org.apache.hadoop.util.StringUtils;

public class MRJobHistoryLoader {
	private static final Log LOG = LogFactory.getLog(MRJobHistoryLoader.class);
	private String jobConfFile;
	private String jobStatFile;
	private MRJobInfo mrJobInfo;
	private ClusterConfiguration cluster;
	private Configuration hadoopConf;
	private boolean detailedDataLoaded;
	private boolean summaryDataLoaded;
	private static final String TASK_COUNTER_GROUP = "org.apache.hadoop.mapred.Task$Counter";
	private static final String FILE_COUNTER_GROUP = "FileSystemCounters";
	private static final String MULTI_STORE_COUNTER_GROUP = "MultiStoreCounters";
	private static final String MAP_TASKS_MAX = "mapred.tasktracker.map.tasks.maximum";
	private static final String RED_TASKS_MAX = "mapred.tasktracker.reduce.tasks.maximum";
	private static final String JOB_TRACKER = "mapred.job.tracker";
	private static final String JOB_TRACKER_PREFIX = "job_tracker_";
	private static final String MASTER_RACK = "/master-rack/";
	private static final String JOB = "JOB";
	private static final String MAP = "MAP";
	private static final String REDUCE = "REDUCE";
	private static final String CLEANUP = "CLEANUP";
	private static final String SETUP = "SETUP";
	private static final String COMMA = ",";
	private static final String EMPTY = "";
	private static final String SLASHES = "//";
	private static final char COLON = ':';
	private static final char EQUALS = '=';
	private static final char ESCAPE_CHAR = '\\';
	private static final char[] CHARS_TO_ESCAPE = { '"', '=', '.' };

	private static final Pattern pattern = Pattern
			.compile("(\\w+)=\"[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*\"");

	public MRJobHistoryLoader(String jobConfFile, String jobStatFile) {
		this.jobConfFile = jobConfFile;
		this.jobStatFile = jobStatFile;
		this.mrJobInfo = new MRJobInfo();
		this.cluster = new ClusterConfiguration();
		this.hadoopConf = null;
		this.detailedDataLoaded = false;
		this.summaryDataLoaded = false;
	}

	public ClusterConfiguration getClusterConfiguration() {
		if ((!this.detailedDataLoaded)
				&& (!loadJobAndClusterData(this.cluster, this.mrJobInfo))) {
			return null;
		}

		return this.cluster;
	}

	public MRJobInfo getMRJobInfoWithDetails() {
		if ((!this.detailedDataLoaded)
				&& (!loadJobAndClusterData(this.cluster, this.mrJobInfo))) {
			return null;
		}

		return this.mrJobInfo;
	}

	public MRJobInfo getMRJobInfoWithSummary() {
		if ((!this.summaryDataLoaded)
				&& (!loadMRJobSummaryData(this.mrJobInfo))) {
			return null;
		}

		return this.mrJobInfo;
	}

	public Configuration getHadoopConfiguration() {
		if (this.hadoopConf == null) {
			this.hadoopConf = new Configuration();
			this.hadoopConf.addResource(new Path(this.jobConfFile));
		}

		return this.hadoopConf;
	}

	public boolean loadClusterConfiguration(ClusterConfiguration cluster) {
		if ((this.detailedDataLoaded == true) && (this.cluster == cluster)) {
			return true;
		}

		this.cluster = cluster;
		return loadJobAndClusterData(this.cluster, this.mrJobInfo);
	}

	public boolean loadMRJobInfoSummary(MRJobInfo mrJobInfo) {
		if ((this.summaryDataLoaded == true) && (this.mrJobInfo == mrJobInfo)) {
			return true;
		}

		this.mrJobInfo = mrJobInfo;
		return loadMRJobSummaryData(this.mrJobInfo);
	}

	public boolean loadMRJobInfoDetails(MRJobInfo mrJobInfo) {
		if ((this.detailedDataLoaded == true) && (this.mrJobInfo == mrJobInfo)) {
			return true;
		}

		this.mrJobInfo = mrJobInfo;
		return loadJobAndClusterData(this.cluster, this.mrJobInfo);
	}

	private boolean loadMRJobSummaryData(MRJobInfo mrJobInfo) {
		FileReader reader = null;
		try {
			reader = new FileReader(this.jobStatFile);
		} catch (FileNotFoundException e) {
			LOG.error("Unable to load job history data", e);
			return false;
		}

		BufferedReader br = new BufferedReader(reader);
		try {
			String line = br.readLine();
			while (line != null) {
				idx = line.indexOf(32);
				if (idx == -1) {
					line = br.readLine();
					continue;
				}
				String recType = line.substring(0, idx);

				if (recType.equalsIgnoreCase("JOB")) {
					String data = line.substring(idx + 1, line.length());
					Matcher matcher = pattern.matcher(data);

					while (matcher.find()) {
						String[] parts = StringUtils.split(matcher.group(0),
								'\\', '=');

						String value = parts[1].substring(1,
								parts[1].length() - 1);

						value = StringUtils.unEscapeString(value, '\\',
								CHARS_TO_ESCAPE);

						setMRJobAttribute(mrJobInfo,
								JobHistory.Keys.valueOf(parts[0]), value);
					}

				}

				line = br.readLine();
			}
		} catch (IOException e) {
			LOG.error("Unable to load job history data", e);
			int idx = 0;
			return idx;
		} finally {
			try {
				br.close();
			} catch (IOException e) {
			}
		}
		this.summaryDataLoaded = true;
		return true;
	}

	private boolean loadJobAndClusterData(ClusterConfiguration cluster,
			MRJobInfo mrJobInfo) {
		this.hadoopConf = new Configuration();
		this.hadoopConf.addResource(new Path(this.jobConfFile));

		JobHistory.JobInfo hadoopJob = new JobHistory.JobInfo("");
		try {
			DefaultJobHistoryParser.parseJobTasks(this.jobStatFile, hadoopJob,
					FileSystem.getLocal(this.hadoopConf));
		} catch (IOException e) {
			LOG.error("Unable to load job history data", e);
			return false;
		}

		populateClusterAndMRJobInfo(cluster, mrJobInfo, hadoopJob,
				this.hadoopConf);
		this.detailedDataLoaded = true;
		this.summaryDataLoaded = true;
		return true;
	}

	private void populateClusterAndMRJobInfo(ClusterConfiguration cluster,
			MRJobInfo mrJobInfo, JobHistory.JobInfo job, Configuration conf) {
		for (Map.Entry entry : job.getValues().entrySet()) {
			setMRJobAttribute(mrJobInfo, (JobHistory.Keys) entry.getKey(),
					(String) entry.getValue());
		}

		for (JobHistory.Task task : job.getAllTasks().values()) {
			if (task.get(JobHistory.Keys.TASK_TYPE).equalsIgnoreCase("MAP")) {
				MRMapInfo mrMapInfo = new MRMapInfo();
				mrJobInfo.addMapTaskInfo(mrMapInfo);
				populateMRMapInfo(cluster, mrMapInfo, task);
			} else if (task.get(JobHistory.Keys.TASK_TYPE).equalsIgnoreCase(
					"REDUCE")) {
				MRReduceInfo mrReduceInfo = new MRReduceInfo();
				mrJobInfo.addReduceTaskInfo(mrReduceInfo);
				populateMRReduceInfo(cluster, mrReduceInfo, task);
			} else if (task.get(JobHistory.Keys.TASK_TYPE).equalsIgnoreCase(
					"SETUP")) {
				MRSetupInfo mrSetupInfo = new MRSetupInfo();
				mrJobInfo.addSetupTaskInfo(mrSetupInfo);
				populateMRSetupInfo(cluster, mrSetupInfo, task);
			} else if (task.get(JobHistory.Keys.TASK_TYPE).equalsIgnoreCase(
					"CLEANUP")) {
				MRCleanupInfo mrCleanupInfo = new MRCleanupInfo();
				mrJobInfo.addCleanupTaskInfo(mrCleanupInfo);
				populateMRCleanupInfo(cluster, mrCleanupInfo, task);
			}

		}

		populateCluster(cluster, conf);
	}

	private void populateMRTaskInfo(ClusterConfiguration cluster, MRTaskInfo mrTaskInfo, JobHistory.Task task)
  {
    for (Map.Entry entry : task.getValues().entrySet())
    {
      switch (1.$SwitchMap$org$apache$hadoop$mapred$JobHistory$Keys[((JobHistory.Keys)entry.getKey()).ordinal()]) {
      case 1:
        mrTaskInfo.setExecId((String)entry.getValue());
        break;
      case 2:
        mrTaskInfo.setStartTime(new Date(Long.parseLong((String)entry.getValue())));

        break;
      case 3:
        mrTaskInfo.setEndTime(new Date(Long.parseLong((String)entry.getValue())));

        break;
      case 4:
        mrTaskInfo.setStatus(MRExecutionStatus.valueOf((String)entry.getValue()));

        break;
      case 5:
        mrTaskInfo.setErrorMsg((String)entry.getValue());
      }
    }
  }

	private void populateMRTaskAttemptInfo(ClusterConfiguration cluster, MRTaskAttemptInfo mrTaskAttemptInfo, JobHistory.TaskAttempt taskAttempt)
  {
    String trackerName = null;
    String fullHostName = null;
    int port = 50060;

    for (Map.Entry entry : taskAttempt.getValues().entrySet())
    {
      switch (1.$SwitchMap$org$apache$hadoop$mapred$JobHistory$Keys[((JobHistory.Keys)entry.getKey()).ordinal()]) {
      case 6:
        mrTaskAttemptInfo.setExecId((String)entry.getValue());
        break;
      case 2:
        mrTaskAttemptInfo.setStartTime(new Date(Long.parseLong((String)entry.getValue())));

        break;
      case 3:
        mrTaskAttemptInfo.setEndTime(new Date(Long.parseLong((String)entry.getValue())));

        break;
      case 4:
        mrTaskAttemptInfo.setStatus(MRExecutionStatus.valueOf((String)entry.getValue()));

        break;
      case 5:
        mrTaskAttemptInfo.setErrorMsg((String)entry.getValue());
        break;
      case 7:
        trackerName = (String)entry.getValue();
        int colIndex = trackerName.indexOf(58);
        if (colIndex == -1) break;
        trackerName = trackerName.substring(0, colIndex); break;
      case 8:
        if (((String)entry.getValue()).equals("")) break;
        port = Integer.parseInt((String)entry.getValue()); break;
      case 9:
        fullHostName = (String)entry.getValue();
        break;
      case 10:
        parseMRTaskAttemptCounters(mrTaskAttemptInfo.getProfile(), (String)entry.getValue());
      }

    }

    mrTaskAttemptInfo.getProfile().setTaskId(mrTaskAttemptInfo.getExecId());

    if ((trackerName != null) && (fullHostName != null)) {
      TaskTrackerInfo tracker = cluster.addFindTaskTrackerInfo(trackerName, fullHostName);

      if (tracker != null) {
        tracker.setPort(port);
        mrTaskAttemptInfo.setTaskTracker(tracker);
      }
    }
  }

	private void populateMRMapInfo(ClusterConfiguration cluster,
			MRMapInfo mrMapInfo, JobHistory.Task task) {
		populateMRTaskInfo(cluster, mrMapInfo, task);

		for (String split : task.get(JobHistory.Keys.SPLITS).split(",")) {
			SlaveHostInfo host = cluster.addFindSlaveHostInfo(split);
			if (host != null) {
				mrMapInfo.addSplitHost(host);
			}

		}

		for (JobHistory.TaskAttempt taskAttempt : task.getTaskAttempts()
				.values()) {
			MRMapAttemptInfo mrMapAttemptInfo = new MRMapAttemptInfo();
			mrMapInfo.addAttempt(mrMapAttemptInfo);
			populateMRTaskAttemptInfo(cluster, mrMapAttemptInfo, taskAttempt);

			SlaveHostInfo host = cluster.addFindSlaveHostInfo(taskAttempt
					.get(JobHistory.Keys.HOSTNAME));

			if (host != null) {
				boolean hostLocal = false;
				boolean rackLocal = false;

				for (SlaveHostInfo splitHost : mrMapInfo.getSplitHosts()) {
					if (splitHost.equals(host))
						hostLocal = true;
					else if (splitHost.getRackName().equals(host.getRackName())) {
						rackLocal = true;
					}

				}

				if (hostLocal)
					mrMapAttemptInfo.setDataLocality(DataLocality.DATA_LOCAL);
				else if (rackLocal)
					mrMapAttemptInfo.setDataLocality(DataLocality.RACK_LOCAL);
				else {
					mrMapAttemptInfo.setDataLocality(DataLocality.NON_LOCAL);
				}

			}

			MRMapProfile mapProf = mrMapAttemptInfo.getProfile();
			if ((!mapProf
					.containsCounter(MRCounter.MAP_OUTPUT_MATERIALIZED_BYTES))
					&& (mapProf.getCounter(MRCounter.SPILLED_RECORDS,
							Long.valueOf(0L)).longValue() != 0L)) {
				mapProf.addCounter(MRCounter.MAP_OUTPUT_MATERIALIZED_BYTES,
						Long.valueOf(mapProf.getCounter(
								MRCounter.FILE_BYTES_WRITTEN, Long.valueOf(0L))
								.longValue()
								- mapProf.getCounter(MRCounter.FILE_BYTES_READ,
										Long.valueOf(0L)).longValue()));
			}
		}
	}

	private void populateMRReduceInfo(ClusterConfiguration cluster,
			MRReduceInfo mrReduceInfo, JobHistory.Task task) {
		populateMRTaskInfo(cluster, mrReduceInfo, task);

		for (JobHistory.TaskAttempt taskAttempt : task.getTaskAttempts()
				.values()) {
			MRReduceAttemptInfo mrReduceAttemptInfo = new MRReduceAttemptInfo();
			mrReduceInfo.addAttempt(mrReduceAttemptInfo);
			populateMRTaskAttemptInfo(cluster, mrReduceAttemptInfo, taskAttempt);

			if (taskAttempt.getValues().containsKey(
					JobHistory.Keys.SHUFFLE_FINISHED)) {
				mrReduceAttemptInfo.setShuffleEndTime(new Date(taskAttempt
						.getLong(JobHistory.Keys.SHUFFLE_FINISHED)));
			}

			if (taskAttempt.getValues().containsKey(
					JobHistory.Keys.SORT_FINISHED))
				mrReduceAttemptInfo.setSortEndTime(new Date(taskAttempt
						.getLong(JobHistory.Keys.SORT_FINISHED)));
		}
	}

	private void populateMRSetupInfo(ClusterConfiguration cluster,
			MRSetupInfo mrSetupInfo, JobHistory.Task task) {
		populateMRTaskInfo(cluster, mrSetupInfo, task);

		for (JobHistory.TaskAttempt taskAttempt : task.getTaskAttempts()
				.values()) {
			MRSetupAttemptInfo mrSetupAttemptInfo = new MRSetupAttemptInfo();
			mrSetupInfo.addAttempt(mrSetupAttemptInfo);
			populateMRTaskAttemptInfo(cluster, mrSetupAttemptInfo, taskAttempt);
		}
	}

	private void populateMRCleanupInfo(ClusterConfiguration cluster,
			MRCleanupInfo mrCleanupInfo, JobHistory.Task task) {
		populateMRTaskInfo(cluster, mrCleanupInfo, task);

		for (JobHistory.TaskAttempt taskAttempt : task.getTaskAttempts()
				.values()) {
			MRCleanupAttemptInfo mrCleanupAttemptInfo = new MRCleanupAttemptInfo();
			mrCleanupInfo.addAttempt(mrCleanupAttemptInfo);
			populateMRTaskAttemptInfo(cluster, mrCleanupAttemptInfo,
					taskAttempt);
		}
	}

	private void populateCluster(ClusterConfiguration cluster,
			Configuration conf) {
		int numMapSlots = conf
				.getInt("mapred.tasktracker.map.tasks.maximum", 2);
		int numReduceSlots = conf.getInt(
				"mapred.tasktracker.reduce.tasks.maximum", 2);
		long mapTaskMem = ProfileUtils.getMapTaskMemory(conf);
		long redTaskMem = ProfileUtils.getReduceTaskMemory(conf);

		for (TaskTrackerInfo taskTrackerInfo : cluster
				.getAllTaskTrackersInfos()) {
			taskTrackerInfo.setNumMapSlots(numMapSlots);
			taskTrackerInfo.setNumReduceSlots(numReduceSlots);
			taskTrackerInfo.setMaxMapTaskMemory(mapTaskMem);
			taskTrackerInfo.setMaxReduceTaskMemory(redTaskMem);
		}

		String jobTracker = conf.get("mapred.job.tracker");
		if (jobTracker != null) {
			int prefix = jobTracker.indexOf("//");
			prefix = prefix == -1 ? 0 : prefix + 2;
			int colIndex = jobTracker.lastIndexOf(58);
			colIndex = (colIndex == -1) || (colIndex <= prefix) ? jobTracker
					.length() : colIndex;

			String hostName = jobTracker.substring(prefix, colIndex);

			JobTrackerInfo jobTrackerInfo = cluster.addFindJobTrackerInfo(
					"job_tracker_" + hostName, "/master-rack/" + hostName);

			if (colIndex < jobTracker.length())
				try {
					jobTrackerInfo.setPort(Integer.parseInt(jobTracker
							.substring(colIndex + 1)));
				} catch (Exception e) {
				}
		}
	}

	private void parseMRTaskAttemptCounters(MRTaskProfile mrTaskProfile,
			String strCounters) {
		Counters counters = null;
		try {
			counters = Counters.fromEscapedCompactString(strCounters);
		} catch (ParseException e) {
			LOG.error("Unable to parse counters string", e);
			return;
		}

		for (Counters.Group group : counters) {
			if ((group.getName()
					.equalsIgnoreCase("org.apache.hadoop.mapred.Task$Counter"))
					|| (group.getName().equalsIgnoreCase("FileSystemCounters"))) {
				for (Counters.Counter counter : group) {
					if (MRCounter.isValid(counter.getName())) {
						mrTaskProfile.addCounter(
								MRCounter.valueOf(counter.getName()),
								Long.valueOf(counter.getValue()));
					} else {
						mrTaskProfile.addAuxCounter(counter.getName(),
								Long.valueOf(counter.getValue()));
					}
				}
			}

		}

		for (Counters.Group group : counters)
			if (group.getName().equalsIgnoreCase("MultiStoreCounters"))
				for (Counters.Counter counter : group) {
					MRCounter outCounter = (mrTaskProfile instanceof MRReduceProfile) ? MRCounter.REDUCE_OUTPUT_RECORDS
							: MRCounter.MAP_OUTPUT_RECORDS;

					long redRecords = mrTaskProfile.getCounter(outCounter,
							Long.valueOf(0L)).longValue();
					mrTaskProfile.addCounter(outCounter,
							Long.valueOf(redRecords + counter.getValue()));
				}
	}

	private void setMRJobAttribute(MRJobInfo mrJobInfo, JobHistory.Keys key, String value)
  {
    switch (1.$SwitchMap$org$apache$hadoop$mapred$JobHistory$Keys[key.ordinal()]) {
    case 11:
      mrJobInfo.setExecId(value);
      break;
    case 12:
      mrJobInfo.setName(value);
      break;
    case 13:
      mrJobInfo.setUser(value);
      break;
    case 14:
      mrJobInfo.setStartTime(new Date(Long.parseLong(value)));
      break;
    case 3:
      mrJobInfo.setEndTime(new Date(Long.parseLong(value)));
      break;
    case 15:
      mrJobInfo.setStatus(MRExecutionStatus.valueOf(value));
      break;
    case 5:
      mrJobInfo.setErrorMsg(value);
      break;
    case 4:
    case 6:
    case 7:
    case 8:
    case 9:
    case 10:
    }
  }
}