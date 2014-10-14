package edu.duke.starfish.profile.profiler;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.IMRInfoManager;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.metrics.Metric;
import edu.duke.starfish.profile.profileinfo.metrics.MetricType;
import edu.duke.starfish.profile.profileinfo.setup.HostInfo;
import edu.duke.starfish.profile.profiler.loaders.MRJobHistoryLoader;
import edu.duke.starfish.profile.profiler.loaders.MRJobProfileLoader;
import edu.duke.starfish.profile.profiler.loaders.MRJobTransfersLoader;
import edu.duke.starfish.profile.profiler.loaders.MRTaskProfilesLoader;
import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class MRJobLogsManager implements IMRInfoManager {
	private static final Log LOG = LogFactory.getLog(MRJobLogsManager.class);
	private String historyDir;
	private String jobProfilesDir;
	private String taskProfilesDir;
	private String transfersDir;
	private Map<String, MRJobHistoryLoader> jobHistories;
	private Map<String, MRJobProfileLoader> jobProfiles;
	private Map<String, MRTaskProfilesLoader> taskProfiles;
	private Map<String, MRJobTransfersLoader> jobTransfers;
	private static final String DOT_XML = ".xml";
	private static final Pattern NAME_PATTERN = Pattern
			.compile(".*(job_[0-9]+_[0-9]+)_.*");

	public MRJobLogsManager() {
		this.historyDir = null;
		this.jobProfilesDir = null;
		this.taskProfilesDir = null;
		this.transfersDir = null;
		this.jobHistories = new HashMap();
		this.jobProfiles = new HashMap();
		this.taskProfiles = new HashMap();
		this.jobTransfers = new HashMap();
	}

	public void setResultsDir(String resultsDir) {
		File dir = new File(resultsDir);
		if (!dir.isDirectory()) {
			LOG.error(dir.getAbsolutePath() + " is not a directory!");
			return;
		}

		File history = new File(resultsDir, "history");
		if (history.exists()) {
			setHistoryDir(history.getAbsolutePath());
		}
		File job_profiles = new File(resultsDir, "job_profiles");
		if (job_profiles.exists()) {
			setJobProfilesDir(job_profiles.getAbsolutePath());
		}
		File task_profiles = new File(resultsDir, "task_profiles");
		if (task_profiles.exists()) {
			setTaskProfilesDir(task_profiles.getAbsolutePath());
		}
		File transfers = new File(resultsDir, "transfers");
		if (transfers.exists())
			setTransfersDir(transfers.getAbsolutePath());
	}

	public void setHistoryDir(String historyDir) {
		this.historyDir = historyDir;
		readHistoryDirectory();
	}

	public void setJobProfilesDir(String jobProfilesDir) {
		this.jobProfilesDir = jobProfilesDir;
	}

	public void setTaskProfilesDir(String taskProfilesDir) {
		this.taskProfilesDir = taskProfilesDir;
	}

	public void setTransfersDir(String transfersDir) {
		this.transfersDir = transfersDir;
	}

	public List<MRJobInfo> getAllMRJobInfos() {
		List jobInfos = new ArrayList();
		for (MRJobHistoryLoader history : this.jobHistories.values()) {
			jobInfos.add(history.getMRJobInfoWithSummary());
		}

		Collections.sort(jobInfos, new Comparator() {
			public int compare(MRJobInfo o1, MRJobInfo o2) {
				return o1.getExecId().compareTo(o2.getExecId());
			}
		});
		return jobInfos;
	}

	public List<MRJobInfo> getAllMRJobInfos(Date start, Date end) {
		List allJobs = new ArrayList();

		for (MRJobInfo job : getAllMRJobInfos()) {
			if ((start.before(job.getStartTime()))
					&& (end.after(job.getEndTime()))) {
				allJobs.add(job);
			}
		}

		return allJobs;
	}

	public MRJobInfo getMRJobInfo(String mrJobId) {
		if (this.jobHistories.containsKey(mrJobId)) {
			return ((MRJobHistoryLoader) this.jobHistories.get(mrJobId))
					.getMRJobInfoWithSummary();
		}
		return null;
	}

	public ClusterConfiguration getClusterConfiguration(String mrJobId) {
		if (this.jobHistories.containsKey(mrJobId)) {
			return ((MRJobHistoryLoader) this.jobHistories.get(mrJobId))
					.getClusterConfiguration();
		}
		return null;
	}

	public Configuration getHadoopConfiguration(String mrJobId) {
		if (this.jobHistories.containsKey(mrJobId)) {
			return ((MRJobHistoryLoader) this.jobHistories.get(mrJobId))
					.getHadoopConfiguration();
		}
		return null;
	}

	public MRJobProfile getMRJobProfile(String mrJobId) {
		MRJobInfo mrJob = getMRJobInfo(mrJobId);
		if ((mrJob != null) && (loadProfilesForMRJob(mrJob))) {
			return mrJob.getProfile();
		}
		return null;
	}

	public List<Metric> getHostMetrics(MetricType type, HostInfo host,
			Date start, Date end) {
		return null;
	}

	public boolean loadTaskDetailsForMRJob(MRJobInfo mrJob) {
		if (this.jobHistories.containsKey(mrJob.getExecId())) {
			return ((MRJobHistoryLoader) this.jobHistories.get(mrJob
					.getExecId())).loadMRJobInfoDetails(mrJob);
		}

		return false;
	}

	public boolean loadDataTransfersForMRJob(MRJobInfo mrJob) {
		if ((this.transfersDir == null) || (!loadTaskDetailsForMRJob(mrJob))) {
			return false;
		}

		if (!this.jobTransfers.containsKey(mrJob.getExecId())) {
			this.jobTransfers.put(mrJob.getExecId(), new MRJobTransfersLoader(
					mrJob, this.transfersDir));
		}

		return ((MRJobTransfersLoader) this.jobTransfers.get(mrJob.getExecId()))
				.loadDataTransfers(mrJob);
	}

	public boolean loadProfilesForMRJob(MRJobInfo mrJob) {
		if ((this.taskProfilesDir == null) && (this.jobProfilesDir == null))
			return false;
		if (!loadTaskDetailsForMRJob(mrJob)) {
			return false;
		}
		String mrJobId = mrJob.getExecId();
		boolean loaded = false;

		if (this.jobProfilesDir != null) {
			if (!this.jobProfiles.containsKey(mrJobId)) {
				this.jobProfiles.put(mrJobId, new MRJobProfileLoader(mrJob,
						this.jobProfilesDir));
			}

			loaded = ((MRJobProfileLoader) this.jobProfiles.get(mrJobId))
					.loadJobProfile(mrJob);
		}

		if (this.taskProfilesDir != null) {
			if (!this.taskProfiles.containsKey(mrJobId)) {
				this.taskProfiles.put(mrJobId, new MRTaskProfilesLoader(mrJob,
						getHadoopConfiguration(mrJobId), this.taskProfilesDir));
			}

			loaded = (((MRTaskProfilesLoader) this.taskProfiles.get(mrJobId))
					.loadExecutionProfile(mrJob)) || (loaded);
		}

		return loaded;
	}

	private void readHistoryDirectory() {
		if (this.historyDir == null) {
			return;
		}

		File dir = new File(this.historyDir);
		if (!dir.isDirectory()) {
			LOG.error(dir.getAbsolutePath() + " is not a directory!");
			return;
		}

		File[] files = dir.listFiles(new FileFilter() {
			public boolean accept(File pathname) {
				return (pathname.isFile())
						&& (!pathname.isHidden())
						&& (MRJobLogsManager.NAME_PATTERN.matcher(pathname
								.getName()).matches());
			}
		});
		Arrays.sort(files);

		File confFile = null;
		File statFile = null;
		for (File file : files) {
			if (file.getName().endsWith(".xml"))
				confFile = file;
			else {
				statFile = file;
			}

			if ((confFile == null) || (statFile == null))
				continue;
			String jobId1 = buildJobId(confFile.getName());
			String jobId2 = buildJobId(statFile.getName());

			if ((jobId1 == null) || (jobId2 == null)
					|| (!jobId1.equalsIgnoreCase(jobId2))) {
				continue;
			}
			this.jobHistories.put(
					jobId1,
					new MRJobHistoryLoader(confFile.getAbsolutePath(), statFile
							.getAbsolutePath()));

			confFile = null;
			statFile = null;
		}
	}

	private String buildJobId(String fileName) {
		Matcher matcher = NAME_PATTERN.matcher(fileName);
		if (matcher.find()) {
			return matcher.group(1);
		}
		return null;
	}
}