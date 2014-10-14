package edu.duke.starfish.profile.profiler.loaders.tasks;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public abstract class MRTaskProfileLoader {
	private static final Log LOG = LogFactory.getLog(MRTaskProfileLoader.class);
	protected MRTaskProfile profile;
	protected Configuration conf;
	protected String profileFile;
	private EnumMap<ProfileToken, ArrayList<ProfileRecord>> records;
	private boolean loaded;
	protected static final List<ProfileRecord> EMPTY_RECORDS = new ArrayList(0);
	protected static final String TAB = "\t";
	protected static final double DEFAULT_COMPR_RATIO = 0.3D;
	protected static final double NS_PER_MS = 1000000.0D;
	protected static final String TOTAL_RUN = "TOTAL_RUN";
	protected static final String SETUP = "SETUP";
	protected static final String CLEANUP = "CLEANUP";
	protected static final String READ = "READ";
	protected static final String WRITE = "WRITE";
	protected static final String COMPRESS = "COMPRESS";
	protected static final String UNCOMPRESS = "UNCOMPRESS";
	protected static final String MAP = "MAP";
	protected static final String REDUCE = "REDUCE";
	protected static final String COMBINE = "COMBINE";
	protected static final String PARTITION_OUTPUT = "PARTITION_OUTPUT";
	protected static final String SERIALIZE_OUTPUT = "SERIALIZE_OUTPUT";
	protected static final String SORT_AND_SPILL = "SORT_AND_SPILL";
	protected static final String QUICK_SORT = "QUICK_SORT";
	protected static final String SORT_COUNT = "SORT_COUNT";
	protected static final String KEY_BYTE_COUNT = "KEY_BYTE_COUNT";
	protected static final String VALUE_BYTE_COUNT = "VALUE_BYTE_COUNT";
	protected static final String UNCOMPRESS_BYTE_COUNT = "UNCOMPRESS_BYTE_COUNT";
	protected static final String COMPRESS_BYTE_COUNT = "COMPRESS_BYTE_COUNT";
	protected static final String TRANSFER_COST = "TRANSFER_COST";
	protected static final String TOTAL_MERGE = "TOTAL_MERGE";
	protected static final String READ_WRITE = "READ_WRITE";
	protected static final String READ_WRITE_COUNT = "READ_WRITE_COUNT";
	protected static final String COPY_MAP_DATA = "COPY_MAP_DATA";
	protected static final String MERGE_MAP_DATA = "MERGE_MAP_DATA";
	protected static final String MERGE_IN_MEMORY = "MERGE_IN_MEMORY";
	protected static final String MERGE_TO_DISK = "MERGE_TO_DISK";
	protected static final String STARTUP_MEM = "STARTUP_MEM";
	protected static final String SETUP_MEM = "SETUP_MEM";
	protected static final String MAP_MEM = "MAP_MEM";
	protected static final String REDUCE_MEM = "REDUCE_MEM";
	protected static final String CLEANUP_MEM = "CLEANUP_MEM";
	protected static final String SPILLED_BYTE_COUNT = "SPILLED_BYTE_COUNT";
	protected static final String SPILLED_BAG_COUNT = "SPILLED_BAG_COUNT";
	protected static final String SPILLED_RECORD_COUNT = "SPILLED_RECORD_COUNT";
	protected static final String SPILLED_NOTIFICATION_COUNT = "SPILLED_NOTIFICATION_COUNT";
	protected static final String SPILLED_ACTUAL_COUNT = "SPILLED_ACTUAL_COUNT";
	protected static final String MAX_MEMORY = "MAX_MEMORY";
	protected static final String MIN_MEMORY = "MIN_MEMORY";
	protected static final String AVG_MEMORY = "AVG_MEMORY";

	public MRTaskProfileLoader(MRTaskProfile profile, Configuration conf,
			String profileFile) {
		this.profile = profile;
		this.conf = conf;
		this.profileFile = profileFile;
		this.records = new EnumMap(ProfileToken.class);

		this.loaded = false;
	}

	public MRTaskProfile getProfile() {
		if (!this.loaded) {
			loadExecutionProfile(this.profile);
		}
		return this.profile;
	}

	public Configuration getConfiguration() {
		return this.conf;
	}

	public String getProfileFile() {
		return this.profileFile;
	}

	public boolean loadExecutionProfile(MRTaskProfile profile) {
		if (!this.profile.getTaskId().equalsIgnoreCase(profile.getTaskId()))
			return false;
		if ((this.loaded) && (this.profile == profile)) {
			return true;
		}

		this.profile = profile;
		this.loaded = true;
		try {
			if (parseProfileFile()) {
				return loadExecutionProfile();
			}
			return false;
		} catch (ProfileFormatException e) {
			LOG.error("Unable to load task profile file", e);
		}
		return false;
	}

	protected abstract boolean loadExecutionProfile()
			throws MRTaskProfileLoader.ProfileFormatException;

	protected List<ProfileRecord> getProfileRecords(ProfileToken token) {
		return this.records.containsKey(token) ? (List) this.records.get(token)
				: EMPTY_RECORDS;
	}

	protected long aggregateRecordValues(List<ProfileRecord> records,
			int groupSize, int pos) {
		long aggr = 0L;

		for (int i = pos; i < records.size(); i += groupSize) {
			aggr += ((ProfileRecord) records.get(i)).getValue();
		}

		return aggr;
	}

	protected double averageRecordValues(List<ProfileRecord> records,
			int groupSize, int pos) {
		double aggr = 0.0D;
		int numGroups = 0;

		for (int i = pos; i < records.size(); i += groupSize) {
			aggr += ((ProfileRecord) records.get(i)).getValue();
			numGroups++;
		}

		return aggr / numGroups;
	}

	protected double averageRecordValueRatios(List<ProfileRecord> records,
			int groupSize, int pos1, int pos2) {
		double sumRatios = 0.0D;
		int numGroups = 0;

		for (int i = 0; i < records.size(); i += groupSize) {
			sumRatios += ((ProfileRecord) records.get(i + pos1)).getValue()
					/ ((ProfileRecord) records.get(i + pos2)).getValue();

			numGroups++;
		}

		return sumRatios / numGroups;
	}

	protected double averageProfileValueDiffRatios(List<ProfileRecord> records,
			int groupSize, int pos1, int pos2, int pos3) {
		double sumRatios = 0.0D;
		int numGroups = 0;

		for (int i = 0; i < records.size(); i += groupSize) {
			sumRatios += (((ProfileRecord) records.get(i + pos1)).getValue() - ((ProfileRecord) records
					.get(i + pos2)).getValue())
					/ ((ProfileRecord) records.get(i + pos3)).getValue();

			numGroups++;
		}

		return sumRatios / numGroups;
	}

	protected void calculateCommonCosts() {
		calcPigProactiveSpillsCosts();
		calcPigSpillableManagerCosts();
	}

	protected void calculateCommonStats() {
		calcMemoryStatistics();
	}

	private void calcMemoryStatistics() {
		List memoryRecords = getProfileRecords(ProfileToken.MEMORY);
		for (ProfileRecord record : memoryRecords) {
			if (record.getProcess().equals("MAX_MEMORY")) {
				double max = record.getValue();
				this.profile.addStatistic(MRStatistics.MAX_MEMORY,
						Double.valueOf(max));
			} else if (record.getProcess().equals("MIN_MEMORY")) {
				double min = record.getValue();
				this.profile.addStatistic(MRStatistics.MIN_MEMORY,
						Double.valueOf(min));
			} else if (record.getProcess().equals("AVG_MEMORY")) {
				double avg = record.getValue();
				this.profile.addStatistic(MRStatistics.AVG_MEMORY,
						Double.valueOf(avg));
			}
		}
	}

	private void calcPigProactiveSpillsCosts() {
		long proactive_bags = 0L;
		long proactive_records = 0L;
		long proactive_bytes = 0L;

		List proactiveSpillRecords = getProfileRecords(ProfileToken.PROACTIVE_SPILL);
		if (!proactiveSpillRecords.isEmpty()) {
			for (ProfileRecord record : proactiveSpillRecords) {
				if (record.getProcess().equals("SPILLED_BAG_COUNT"))
					proactive_bags += record.getValue();
				else if (record.getProcess().equals("SPILLED_RECORD_COUNT"))
					proactive_records += record.getValue();
				else if (record.getProcess().equals("SPILLED_BYTE_COUNT")) {
					proactive_bytes += record.getValue();
				}

			}

			if (proactive_bags != 0L) {
				this.profile.addCounter(MRCounter.PIG_ROACTIVE_SPILL_BAGS,
						Long.valueOf(proactive_bags));
			}
			if (proactive_records != 0L) {
				this.profile.addCounter(MRCounter.PIG_ROACTIVE_SPILL_RECORDS,
						Long.valueOf(proactive_records));
			}
			if (proactive_bytes != 0L)
				this.profile.addCounter(MRCounter.PIG_PROACTIVE_SPILL_BYTES,
						Long.valueOf(proactive_bytes));
		}
	}

	private void calcPigSpillableManagerCosts() {
		long spillable_bags = 0L;
		long spillable_records = 0L;
		long spillable_bytes = 0L;
		long spillable_notification_count = 0L;
		long spillable_actual_count = 0L;

		List spillableMemoryManagerRecords = getProfileRecords(ProfileToken.SPILLABLE_MEMORY_MANAGER);
		if (!spillableMemoryManagerRecords.isEmpty()) {
			for (ProfileRecord record : spillableMemoryManagerRecords) {
				if (record.getProcess().equals("SPILLED_BAG_COUNT"))
					spillable_bags += record.getValue();
				else if (record.getProcess().equals("SPILLED_RECORD_COUNT"))
					spillable_records += record.getValue();
				else if (record.getProcess().equals("SPILLED_BYTE_COUNT"))
					spillable_bytes += record.getValue();
				else if (record.getProcess().equals(
						"SPILLED_NOTIFICATION_COUNT")) {
					spillable_notification_count += record.getValue();
				} else if (record.getProcess().equals("SPILLED_ACTUAL_COUNT")) {
					spillable_actual_count += record.getValue();
				}

			}

			if (spillable_bags != 0L) {
				this.profile.addCounter(MRCounter.PIG_MANAGER_SPILL_BAGS,
						Long.valueOf(spillable_bags));
			}
			if (spillable_records != 0L) {
				this.profile.addCounter(MRCounter.PIG_MANAGER_SPILL_RECORDS,
						Long.valueOf(spillable_records));
			}
			if (spillable_bytes != 0L) {
				this.profile.addCounter(MRCounter.PIG_MANAGER_SPILL_BYTES,
						Long.valueOf(spillable_bytes));
			}
			if (spillable_notification_count != 0L) {
				this.profile.addCounter(
						MRCounter.PIG_MANAGER_SPILL_NOTIFICATION_COUNT,
						Long.valueOf(spillable_notification_count));
			}

			if (spillable_actual_count != 0L)
				this.profile.addCounter(
						MRCounter.PIG_MANAGER_SPILL_ACTUAL_COUNT,
						Long.valueOf(spillable_actual_count));
		}
	}

	private boolean parseProfileFile()
			throws MRTaskProfileLoader.ProfileFormatException {
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(new File(this.profileFile)));
		} catch (FileNotFoundException e) {
			LOG.error("Unable to find file: " + this.profileFile);
			return false;
		}

		String line = "";
		try {
			while ((line = br.readLine()) != null) {
				String[] tokens = line.split("\t");
				if (tokens[0].equals("MEM_TRACE")) {
					int i = 0;
					return i;
				}
				if (!ProfileToken.access$000(tokens[0])) {
					LOG.error("Invalid profile line: " + line);
					continue;
				}

				ProfileToken token = ProfileToken.valueOf(tokens[0]);
				if (!this.records.containsKey(token)) {
					this.records.put(token, new ArrayList());
				}
				((ArrayList) this.records.get(token)).add(new ProfileRecord(
						token, tokens[1], Long.parseLong(tokens[2])));
			}
		} catch (IOException e) {
			LOG.error("Unable to parse task profile file", e);
			e = 0;
			return e;
		} catch (RuntimeException e) {
			throw new ProfileFormatException("Invalid profile line: " + line, e);
		} finally {
			try {
				br.close();
			} catch (IOException e) {
			}
		}
		return true;
	}

	public static class ProfileFormatException extends Exception {
		private static final long serialVersionUID = 6908355768995089420L;

		public ProfileFormatException(String message) {
			super();
		}

		public ProfileFormatException(String message, Throwable cause) {
			super(cause);
		}
	}

	public static enum ProfileToken {
		TASK, MAP, SPILL, MERGE, SHUFFLE, SORT, REDUCE, MEMORY, MEM_TRACE, SPILLABLE_MEMORY_MANAGER, PROACTIVE_SPILL;

		private static boolean isValid(String token) {
			try {
				valueOf(token);
			} catch (RuntimeException e) {
				return false;
			}

			return true;
		}
	}

	public static class ProfileRecord implements Comparable<ProfileRecord> {
		private MRTaskProfileLoader.ProfileToken token;
		private String process;
		private long value;

		public ProfileRecord(MRTaskProfileLoader.ProfileToken phase,
				String process, long value) {
			this.token = phase;
			this.process = process;
			this.value = value;
		}

		public int compareTo(ProfileRecord other) {
			return this.token.compareTo(other.token);
		}

		public int hashCode() {
			return this.token.hashCode();
		}

		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof ProfileRecord))
				return false;
			ProfileRecord other = (ProfileRecord) obj;
			return this.token.equals(other.getPhase());
		}

		public String toString() {
			return "ProfileRecord [token=" + this.token + ", process="
					+ this.process + ", value=" + this.value + "]";
		}

		public MRTaskProfileLoader.ProfileToken getPhase() {
			return this.token;
		}

		public String getProcess() {
			return this.process;
		}

		public long getValue() {
			return this.value;
		}
	}
}