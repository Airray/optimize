package edu.duke.starfish.profile.profiler.loaders;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profiler.loaders.tasks.MRMapProfileLoader;
import edu.duke.starfish.profile.profiler.loaders.tasks.MRReduceProfileLoader;
import edu.duke.starfish.profile.utils.ProfileUtils;
import java.io.File;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class MRTaskProfilesLoader {
	private static final Log LOG = LogFactory
			.getLog(MRTaskProfilesLoader.class);
	private MRJobInfo mrJob;
	private Configuration conf;
	private String inputDir;
	private boolean loaded;
	private static String PROFILE_OUT = "profile.out";
	private static String DOT_PROFILE = ".profile";

	public MRTaskProfilesLoader(MRJobInfo mrJob, Configuration conf,
			String inputDir) {
		this.mrJob = mrJob;
		this.conf = conf;
		this.inputDir = inputDir;
		this.loaded = false;
	}

	public MRJobProfile getProfile() {
		if (!this.loaded) {
			loadExecutionProfile(this.mrJob);
		}
		return this.mrJob.getProfile();
	}

	public Configuration getConf() {
		return this.conf;
	}

	public String getInputDir() {
		return this.inputDir;
	}

	public boolean loadExecutionProfile(MRJobInfo mrJob) {
		if (!this.mrJob.getExecId().equalsIgnoreCase(mrJob.getExecId()))
			return false;
		if ((this.loaded) && (this.mrJob == mrJob)) {
			return true;
		}

		this.mrJob = mrJob;

		File filesDir = new File(this.inputDir);
		if (!filesDir.isDirectory()) {
			LOG.error(filesDir.getAbsolutePath() + " is not a directory!");
			return false;
		}

		MRJobProfile profile = new MRJobProfile(mrJob.getExecId());
		boolean success = false;

		for (MRMapAttemptInfo mrMap : mrJob
				.getMapAttempts(MRExecutionStatus.SUCCESS)) {
			success = (loadTaskExecutionProfile(profile, filesDir, mrMap, true))
					|| (success);
		}

		for (MRReduceAttemptInfo mrReduce : mrJob
				.getReduceAttempts(MRExecutionStatus.SUCCESS)) {
			success = (loadTaskExecutionProfile(profile, filesDir, mrReduce,
					false)) || (success);
		}

		if (success) {
			profile.setJobInputs(ProfileUtils.getInputDirs(this.conf));
			profile.updateProfile();

			profile.addCounter(MRCounter.MAP_TASKS,
					Long.valueOf(mrJob.getMapTasks().size()));

			profile.addCounter(MRCounter.REDUCE_TASKS,
					Long.valueOf(mrJob.getReduceTasks().size()));

			String clusterName = mrJob.getProfile().getClusterName();
			if (clusterName != null) {
				profile.setClusterName(clusterName);
			}

			mrJob.setProfile(profile);
		}

		this.loaded = success;
		return success;
	}

	private boolean loadTaskExecutionProfile(MRJobProfile profile,
			File filesDir, MRTaskAttemptInfo task, boolean isMapTask) {
		File profileFile = null;
		File attemptDir = new File(filesDir, task.getExecId());
		if (attemptDir.isDirectory())
			profileFile = new File(attemptDir, PROFILE_OUT);
		else {
			profileFile = new File(filesDir, task.getExecId() + DOT_PROFILE);
		}

		if (!profileFile.exists()) {
			return false;
		}

		if (isMapTask) {
			MRMapProfile mapProfile = (MRMapProfile) task.getProfile();
			MRMapProfileLoader loader = new MRMapProfileLoader(mapProfile,
					this.conf, profileFile.getAbsolutePath());

			if (loader.loadExecutionProfile(mapProfile)) {
				profile.addMapProfile(mapProfile);
				return true;
			}
		} else {
			MRReduceProfile reduceProfile = (MRReduceProfile) task.getProfile();
			MRReduceProfileLoader loader = new MRReduceProfileLoader(
					reduceProfile, this.conf, profileFile.getAbsolutePath());

			if (loader.loadExecutionProfile(reduceProfile)) {
				profile.addReduceProfile(reduceProfile);
				return true;
			}
		}
		return false;
	}
}