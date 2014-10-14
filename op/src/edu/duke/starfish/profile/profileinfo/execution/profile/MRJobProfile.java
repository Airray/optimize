package edu.duke.starfish.profile.profileinfo.execution.profile;

import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MRJobProfile extends MRExecProfile {
	private String jobId;
	private String clusterName;
	private String[] jobInputs;
	private List<MRMapProfile> mapProfiles;
	private List<MRReduceProfile> reduceProfiles;
	private List<MRMapProfile> avgMapProfiles;
	private MRReduceProfile avgReduceProfile;
	private int maxInputIndex;
	private static final String AVG_MAP = "average_map_";
	private static final String AVG_REDUCE = "average_reduce_";
	private static final MRCostFactors[] missingMapCosts = {
			MRCostFactors.READ_LOCAL_IO_COST,
			MRCostFactors.WRITE_LOCAL_IO_COST, MRCostFactors.MERGE_CPU_COST,
			MRCostFactors.INTERM_UNCOMPRESS_CPU_COST };

	private static final MRCostFactors[] missingReduceCosts = {
			MRCostFactors.READ_LOCAL_IO_COST,
			MRCostFactors.WRITE_LOCAL_IO_COST, MRCostFactors.COMBINE_CPU_COST,
			MRCostFactors.MERGE_CPU_COST,
			MRCostFactors.INTERM_UNCOMPRESS_CPU_COST,
			MRCostFactors.INTERM_COMPRESS_CPU_COST };

	private static final MRStatistics[] missingReduceStats = {
			MRStatistics.COMBINE_SIZE_SEL, MRStatistics.COMBINE_PAIRS_SEL };

	public MRJobProfile(String jobId) {
		this.jobId = jobId;
		this.clusterName = null;
		this.jobInputs = null;
		this.mapProfiles = null;
		this.reduceProfiles = null;
		this.avgMapProfiles = null;
		this.avgReduceProfile = null;
		this.maxInputIndex = 0;
	}

	public MRJobProfile(MRJobProfile other) {
		super(other);

		this.jobId = other.jobId;
		this.clusterName = other.clusterName;
		if (other.jobInputs != null) {
			this.jobInputs = new String[other.jobInputs.length];
			for (int i = 0; i < other.jobInputs.length; i++) {
				this.jobInputs[i] = other.jobInputs[i];
			}
		}
		if (other.mapProfiles != null) {
			this.mapProfiles = new ArrayList(other.mapProfiles.size());
			for (MRMapProfile prof : other.mapProfiles)
				this.mapProfiles.add(new MRMapProfile(prof));
		} else {
			this.mapProfiles = null;
		}

		if (other.reduceProfiles != null) {
			this.reduceProfiles = new ArrayList(other.reduceProfiles.size());

			for (MRReduceProfile prof : other.reduceProfiles)
				this.reduceProfiles.add(new MRReduceProfile(prof));
		} else {
			this.reduceProfiles = null;
		}

		if (other.avgMapProfiles != null) {
			this.avgMapProfiles = new ArrayList(other.avgMapProfiles.size());

			for (MRMapProfile prof : other.avgMapProfiles)
				this.avgMapProfiles.add(new MRMapProfile(prof));
		} else {
			this.avgMapProfiles = null;
		}

		if (other.avgReduceProfile != null)
			this.avgReduceProfile = new MRReduceProfile(other.avgReduceProfile);
		else {
			this.avgReduceProfile = null;
		}

		this.maxInputIndex = other.maxInputIndex;
	}

	public String getClusterName() {
		return this.clusterName;
	}

	public String getJobId() {
		return this.jobId;
	}

	public String[] getJobInputs() {
		return this.jobInputs == null ? new String[0] : this.jobInputs;
	}

	public List<MRMapProfile> getMapProfiles() {
		if (this.mapProfiles == null)
			this.mapProfiles = new ArrayList(0);
		return this.mapProfiles;
	}

	public List<MRReduceProfile> getReduceProfiles() {
		if (this.reduceProfiles == null)
			this.reduceProfiles = new ArrayList(0);
		return this.reduceProfiles;
	}

	public List<MRMapProfile> getAvgMapProfiles() {
		initializeAvgMapProfiles();
		return this.avgMapProfiles;
	}

	public MRReduceProfile getAvgReduceProfile() {
		if (this.avgReduceProfile == null)
			this.avgReduceProfile = new MRReduceProfile(new StringBuilder()
					.append("average_reduce_").append(this.jobId).toString());
		return this.avgReduceProfile;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public void setJobInputs(String[] jobInputs) {
		this.jobInputs = jobInputs;
	}

	public void addMapProfile(MRMapProfile mapProfile) {
		if (this.mapProfiles == null)
			this.mapProfiles = new ArrayList();
		this.mapProfiles.add(mapProfile);

		if (mapProfile.getInputIndex() > this.maxInputIndex)
			this.maxInputIndex = mapProfile.getInputIndex();
	}

	public void addReduceProfile(MRReduceProfile reduceProfile) {
		if (this.reduceProfiles == null)
			this.reduceProfiles = new ArrayList();
		this.reduceProfiles.add(reduceProfile);
	}

	public void printProfile(PrintStream out, boolean printTaskProfiles) {
		out.println(new StringBuilder().append("JOB PROFILE:\n\tID:\t")
				.append(this.jobId).toString());

		if (this.clusterName != null) {
			out.println(new StringBuilder().append("\tCluster Name:\t")
					.append(this.clusterName).toString());
		}

		if (this.jobInputs != null) {
			for (int i = 0; i < this.jobInputs.length; i++) {
				out.println(new StringBuilder().append("\tInput Path ")
						.append(i).append(":\t").append(this.jobInputs[i])
						.toString());
			}
		}

		out.println(new StringBuilder().append("\tTotal Mappers:\t")
				.append(getCounter(MRCounter.MAP_TASKS, Long.valueOf(0L)))
				.toString());
		out.println(new StringBuilder().append("\tProfiled Mappers:\t")
				.append(this.mapProfiles == null ? 0 : this.mapProfiles.size())
				.toString());

		out.println(new StringBuilder().append("\tTotal Reducers:\t")
				.append(getCounter(MRCounter.REDUCE_TASKS, Long.valueOf(0L)))
				.toString());

		out.println(new StringBuilder()
				.append("\tProfiled Reducers:\t")
				.append(this.reduceProfiles == null ? 0 : this.reduceProfiles
						.size()).toString());

		out.println("");

		if (this.avgMapProfiles != null) {
			for (MRMapProfile avgMapProfile : this.avgMapProfiles)
				avgMapProfile.printProfile(out);
		}
		if (this.avgReduceProfile != null) {
			this.avgReduceProfile.printProfile(out);
		}

		if (printTaskProfiles) {
			if (this.mapProfiles != null) {
				for (MRMapProfile mapProfile : this.mapProfiles)
					mapProfile.printProfile(out);
			}
			if (this.reduceProfiles != null) {
				for (MRReduceProfile reduceProfile : this.reduceProfiles)
					reduceProfile.printProfile(out);
			}
			out.println("");
		}
	}

	public void updateProfile() {
		long numMappers = getCounter(MRCounter.MAP_TASKS, Long.valueOf(0L))
				.longValue();
		long numReducers = getCounter(MRCounter.REDUCE_TASKS, Long.valueOf(0L))
				.longValue();

		List allProfiles = new ArrayList();
		if (this.mapProfiles != null)
			allProfiles.addAll(this.mapProfiles);
		if (this.reduceProfiles != null)
			allProfiles.addAll(this.reduceProfiles);
		long maxUniqueGroups;
		if ((this.mapProfiles != null) && (this.reduceProfiles != null)) {
			maxUniqueGroups = 0L;
			for (MRReduceProfile redProfile : this.reduceProfiles) {
				maxUniqueGroups += redProfile.getNumTasks()
						* redProfile.getCounter(MRCounter.REDUCE_INPUT_GROUPS,
								Long.valueOf(1L)).longValue();
			}

			for (MRMapProfile mapProfile : this.mapProfiles) {
				mapProfile.addCounter(MRCounter.MAP_MAX_UNIQUE_GROUPS,
						Long.valueOf(maxUniqueGroups));
			}

		}

		updateExecProfile(this, allProfiles);

		if (this.mapProfiles != null) {
			List avgProfiles = getAvgMapProfiles();
			List sepProfiles = separateMapProfilesBasedOnInput();
			for (int i = 0; i < avgProfiles.size(); i++) {
				MRMapProfile avgMapProfile = (MRMapProfile) avgProfiles.get(i);
				updateTaskProfile(avgMapProfile, (List) sepProfiles.get(i));

				if (((List) sepProfiles.get(i)).size() != 0) {
					avgMapProfile
							.setInputIndex(((MRMapProfile) ((List) sepProfiles
									.get(i)).get(0)).getInputIndex());
				}

				for (MRCostFactors cost : missingMapCosts) {
					if ((!containsCostFactor(cost))
							|| (avgMapProfile.containsCostFactor(cost)))
						continue;
					avgMapProfile.addCostFactor(cost, getCostFactor(cost));
				}

			}

		}

		if (this.reduceProfiles != null) {
			MRReduceProfile avgRedProfile = getAvgReduceProfile();
			updateTaskProfile(avgRedProfile, this.reduceProfiles);

			for (MRStatistics stat : missingReduceStats) {
				if ((!containsStatistic(stat))
						|| (avgRedProfile.containsStatistic(stat)))
					continue;
				avgRedProfile.addStatistic(stat, getStatistic(stat));
			}

			for (MRCostFactors cost : missingReduceCosts) {
				if ((!containsCostFactor(cost))
						|| (avgRedProfile.containsCostFactor(cost)))
					continue;
				avgRedProfile.addCostFactor(cost, getCostFactor(cost));
			}

		}

		addCounter(MRCounter.MAP_TASKS, Long.valueOf(numMappers));
		addCounter(MRCounter.REDUCE_TASKS, Long.valueOf(numReducers));
	}

	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (this.jobId == null ? 0 : this.jobId.hashCode());
		result = 37 * result
				+ (this.mapProfiles == null ? 0 : this.mapProfiles.hashCode());

		result = 41
				* result
				+ (this.reduceProfiles == null ? 0 : this.reduceProfiles
						.hashCode());

		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof MRJobProfile))
			return false;
		MRJobProfile other = (MRJobProfile) obj;
		if (this.jobId == null) {
			if (other.jobId != null)
				return false;
		} else if (!this.jobId.equals(other.jobId))
			return false;
		if (this.mapProfiles == null) {
			if (other.mapProfiles != null)
				return false;
		} else if (!this.mapProfiles.equals(other.mapProfiles))
			return false;
		if (this.reduceProfiles == null) {
			if (other.reduceProfiles != null)
				return false;
		} else if (!this.reduceProfiles.equals(other.reduceProfiles))
			return false;
		return true;
	}

	public String toString() {
		return new StringBuilder().append("MRJobProfile [job=")
				.append(this.jobId).append(", counters=")
				.append(getCounters().size()).append(", stats=")
				.append(getStatistics().size()).append(", costs=")
				.append(getCostFactors().size()).append("]").toString();
	}

	private void averageAuxCounters(MRTaskProfile avgProfile,
			List<? extends MRTaskProfile> taskProfiles) {
		Set auxCounters = null;
		for (MRTaskProfile taskProfile : taskProfiles) {
			if (taskProfile.containsAuxCounters()) {
				auxCounters = taskProfile.getAuxCounters().keySet();
				break;
			}
		}

		if (auxCounters == null) {
			return;
		}
		Map avgAuxCounters = avgProfile.getAuxCounters();

		for (String counter : auxCounters) {
			double sumValues = 0.0D;
			int numValues = 0;

			for (MRTaskProfile taskProfile : taskProfiles) {
				if (taskProfile.containsAuxCounter(counter)) {
					sumValues += taskProfile.getNumTasks()
							* taskProfile.getAuxCounter(counter,
									Long.valueOf(0L)).longValue();

					numValues += taskProfile.getNumTasks();
				}

			}

			if (numValues != 0)
				avgAuxCounters.put(counter,
						Long.valueOf(Math.round(sumValues / numValues)));
		}
	}

	private void averageCounters(Map<MRCounter, Long> counters,
			List<? extends MRTaskProfile> taskProfiles) {
		for (MRCounter counter : MRCounter.values()) {
			double sumValues = 0.0D;
			int numValues = 0;

			for (MRTaskProfile taskProfile : taskProfiles) {
				if (taskProfile.containsCounter(counter)) {
					sumValues += taskProfile.getNumTasks()
							* taskProfile.getCounter(counter, Long.valueOf(0L))
									.longValue();

					numValues += taskProfile.getNumTasks();
				}

			}

			if (numValues != 0)
				counters.put(counter,
						Long.valueOf(Math.round(sumValues / numValues)));
		}
	}

	private void averageStatistics(Map<MRStatistics, Double> stats,
			List<? extends MRTaskProfile> taskProfiles) {
		for (MRStatistics stat : MRStatistics.values()) {
			double sumValues = 0.0D;
			int numValues = 0;

			for (MRTaskProfile taskProfile : taskProfiles) {
				if (taskProfile.containsStatistic(stat)) {
					sumValues += taskProfile.getNumTasks()
							* taskProfile.getStatistic(stat,
									Double.valueOf(0.0D)).doubleValue();

					numValues += taskProfile.getNumTasks();
				}

			}

			if (numValues != 0)
				stats.put(stat, Double.valueOf(sumValues / numValues));
		}
	}

	private void averageCostFactors(Map<MRCostFactors, Double> costs,
			List<? extends MRTaskProfile> taskProfiles) {
		for (MRCostFactors cost : MRCostFactors.values()) {
			double sumValues = 0.0D;
			int numValues = 0;

			for (MRTaskProfile taskProfile : taskProfiles) {
				if (taskProfile.containsCostFactor(cost)) {
					sumValues += taskProfile.getNumTasks()
							* taskProfile.getCostFactor(cost,
									Double.valueOf(0.0D)).doubleValue();

					numValues += taskProfile.getNumTasks();
				}

			}

			if (numValues != 0)
				costs.put(cost, Double.valueOf(sumValues / numValues));
		}
	}

	private void averageTimings(Map<MRTaskPhase, Double> timings,
			List<? extends MRTaskProfile> taskProfiles) {
		for (MRTaskPhase phase : MRTaskPhase.values()) {
			double sumValues = 0.0D;
			int numValues = 0;

			for (MRTaskProfile taskProfile : taskProfiles) {
				if (taskProfile.containsTiming(phase)) {
					sumValues += taskProfile.getNumTasks()
							* taskProfile
									.getTiming(phase, Double.valueOf(0.0D))
									.doubleValue();

					numValues += taskProfile.getNumTasks();
				}

			}

			if (numValues != 0)
				timings.put(phase, Double.valueOf(sumValues / numValues));
		}
	}

	private void initializeAvgMapProfiles() {
		int numProfiles = this.maxInputIndex + 1;
		if ((this.avgMapProfiles == null)
				|| (this.avgMapProfiles.size() != numProfiles)) {
			this.avgMapProfiles = new ArrayList(numProfiles);

			for (int i = 0; i < numProfiles; i++)
				this.avgMapProfiles.add(new MRMapProfile(new StringBuilder()
						.append("average_map_").append(i).append("_")
						.append(this.jobId).toString()));
		}
	}

	private List<List<MRMapProfile>> separateMapProfilesBasedOnInput() {
		int numProfiles = this.maxInputIndex + 1;
		List sepProfiles = new ArrayList(numProfiles);

		if (numProfiles == 1) {
			sepProfiles.add(this.mapProfiles);
		} else {
			for (int i = 0; i < numProfiles; i++) {
				sepProfiles.add(new ArrayList());
			}

			for (MRMapProfile mapProfile : this.mapProfiles) {
				((List) sepProfiles.get(mapProfile.getInputIndex()))
						.add(mapProfile);
			}
		}

		return sepProfiles;
	}

	private void updateExecProfile(MRExecProfile profile,
			List<? extends MRTaskProfile> taskProfiles) {
		profile.clearProfile();

		averageCounters(profile.getCounters(), taskProfiles);
		averageStatistics(profile.getStatistics(), taskProfiles);
		averageCostFactors(profile.getCostFactors(), taskProfiles);

		aggregateMemoryStatistics(profile.getStatistics(), taskProfiles);
	}

	private void updateTaskProfile(MRTaskProfile profile,
			List<? extends MRTaskProfile> taskProfiles) {
		profile.clearProfile();
		updateExecProfile(profile, taskProfiles);
		averageTimings(profile.getTimings(), taskProfiles);
		averageAuxCounters(profile, taskProfiles);

		int numTasks = 0;
		for (MRTaskProfile prof : taskProfiles) {
			numTasks += prof.getNumTasks();
		}
		profile.setNumTasks(numTasks);
	}

	private void aggregateMemoryStatistics(Map<MRStatistics, Double> stats,
			List<? extends MRTaskProfile> taskProfiles) {
		double maxValues = 4.9E-324D;
		double minValues = 1.7976931348623157E+308D;
		double sumValues = 0.0D;
		int numValues = 0;

		for (MRTaskProfile prof : taskProfiles) {
			if ((prof.containsStatistic(MRStatistics.MAX_MEMORY))
					&& (maxValues < prof.getStatistic(MRStatistics.MAX_MEMORY)
							.doubleValue())) {
				maxValues = prof.getStatistic(MRStatistics.MAX_MEMORY)
						.doubleValue();
			}

			if (prof.containsStatistic(MRStatistics.AVG_MEMORY)) {
				sumValues += prof.getNumTasks()
						* prof.getStatistic(MRStatistics.AVG_MEMORY)
								.doubleValue();

				numValues += prof.getNumTasks();
			}

			if ((prof.containsStatistic(MRStatistics.MIN_MEMORY))
					&& (minValues > prof.getStatistic(MRStatistics.MIN_MEMORY)
							.doubleValue())) {
				minValues = prof.getStatistic(MRStatistics.MIN_MEMORY)
						.doubleValue();
			}

		}

		if (maxValues != 4.9E-324D)
			stats.put(MRStatistics.MAX_MEMORY, Double.valueOf(maxValues));
		if (minValues != 1.7976931348623157E+308D)
			stats.put(MRStatistics.MIN_MEMORY, Double.valueOf(minValues));
		if (numValues != 0)
			stats.put(MRStatistics.AVG_MEMORY,
					Double.valueOf(sumValues / numValues));
	}
}