package edu.duke.starfish.profile.profileinfo;

import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.metrics.Metric;
import edu.duke.starfish.profile.profileinfo.metrics.MetricType;
import edu.duke.starfish.profile.profileinfo.setup.HostInfo;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

public abstract interface IMRInfoManager {
	public abstract List<MRJobInfo> getAllMRJobInfos();

	public abstract List<MRJobInfo> getAllMRJobInfos(Date paramDate1,
			Date paramDate2);

	public abstract MRJobInfo getMRJobInfo(String paramString);

	public abstract ClusterConfiguration getClusterConfiguration(
			String paramString);

	public abstract Configuration getHadoopConfiguration(String paramString);

	public abstract MRJobProfile getMRJobProfile(String paramString);

	public abstract List<Metric> getHostMetrics(MetricType paramMetricType,
			HostInfo paramHostInfo, Date paramDate1, Date paramDate2);

	public abstract boolean loadTaskDetailsForMRJob(MRJobInfo paramMRJobInfo);

	public abstract boolean loadDataTransfersForMRJob(MRJobInfo paramMRJobInfo);

	public abstract boolean loadProfilesForMRJob(MRJobInfo paramMRJobInfo);
}