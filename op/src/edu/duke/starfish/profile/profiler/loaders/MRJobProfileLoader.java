package edu.duke.starfish.profile.profiler.loaders;

import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.utils.XMLProfileParser;
import java.io.File;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MRJobProfileLoader {
	private static final Log LOG = LogFactory.getLog(MRJobProfileLoader.class);
	private MRJobInfo mrJob;
	private String jobProfilesDir;
	private boolean loaded;

	public MRJobProfileLoader(MRJobInfo mrJob, String jobProfilesDir) {
		this.mrJob = mrJob;
		this.jobProfilesDir = jobProfilesDir;
		this.loaded = false;
	}

	public MRJobProfile getProfile() {
		if (!this.loaded) {
			loadJobProfile(this.mrJob);
		}
		return this.mrJob.getProfile();
	}

	public String getJobProfilesDir() {
		return this.jobProfilesDir;
	}

	public boolean loadJobProfile(MRJobInfo mrJob) {
		if (!this.mrJob.getExecId().equalsIgnoreCase(mrJob.getExecId()))
			return false;
		if ((this.loaded) && (this.mrJob == mrJob)) {
			return true;
		}

		this.mrJob = mrJob;

		File jobProfDir = new File(this.jobProfilesDir);
		if (!jobProfDir.isDirectory()) {
			LOG.error(jobProfDir.getAbsolutePath() + " is not a directory!");
			return false;
		}

		boolean success = false;
		String jobId = mrJob.getExecId();
		File adjProfileXML = new File(jobProfDir, "adj_profile_" + jobId
				+ ".xml");

		if (adjProfileXML.exists()) {
			XMLProfileParser parser = new XMLProfileParser();
			MRJobProfile jobProfile = (MRJobProfile) parser
					.importXML(adjProfileXML);
			mrJob.setAdjProfile(jobProfile);
			mrJob.setProfile(jobProfile);
			success = true;
		}

		File profileXML = new File(jobProfDir, "profile_" + jobId + ".xml");
		if (profileXML.exists()) {
			XMLProfileParser parser = new XMLProfileParser();
			MRJobProfile jobProfile = (MRJobProfile) parser
					.importXML(profileXML);
			mrJob.setProfile(jobProfile);
			success = true;
		}

		this.loaded = success;
		return success;
	}
}