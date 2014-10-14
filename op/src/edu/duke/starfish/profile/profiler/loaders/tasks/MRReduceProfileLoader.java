package edu.duke.starfish.profile.profiler.loaders.tasks;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;
import edu.duke.starfish.profile.utils.ProfileUtils;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

public class MRReduceProfileLoader extends MRTaskProfileLoader {
	private List<MRTaskProfileLoader.ProfileRecord> shuffleRecords;
	private List<MRTaskProfileLoader.ProfileRecord> reduceRecords;
	private List<MRTaskProfileLoader.ProfileRecord> sortRecords;
	private List<MRTaskProfileLoader.ProfileRecord> mergeRecords;
	private static final int NUM_SHUFFLE_PHASES = 4;
	private static final int POS_SHUFFLE_UNCOMPR_BYTE_COUNT = 0;
	private static final int POS_SHUFFLE_COMPR_BYTE_COUNT = 1;
	private static final int POS_SHUFFLE_COPY_MAP_OUTPUT = 2;
	private static final int POS_SHUFFLE_UNCOMPRESS = 3;
	private static final int NUM_MERGE_PHASES = 7;
	private static final int POS_MERGE_MERGE = 0;
	private static final int POS_MERGE_READ_WRITE = 1;
	private static final int POS_MERGE_READ_WRITE_COUNT = 2;
	private static final int POS_MERGE_COMBINE = 3;
	private static final int POS_MERGE_WRITE = 4;
	private static final int POS_MERGE_UNCOMPRESS = 5;
	private static final int POS_MERGE_COMPRESS = 6;
	private static final int NUM_SORT_PHASES = 5;
	private static final int POS_SORT_MERGE = 0;
	private static final int POS_SORT_READ_WRITE = 1;
	private static final int POS_SORT_READ_WRITE_COUNT = 2;
	private static final int POS_SORT_UNCOMPRESS = 3;
	private static final int POS_SORT_COMPRESS = 4;
	private static final int NUM_REDUCE_PHASES = 16;
	private static final int POS_REDUCE_STARTUP_MEM = 0;
	private static final int POS_REDUCE_SETUP = 1;
	private static final int POS_REDUCE_SETUP_MEM = 2;
	private static final int POS_REDUCE_CLEANUP = 3;
	private static final int POS_REDUCE_CLEANUP_MEM = 4;
	private static final int POS_REDUCE_TOTAL_RUN = 5;
	private static final int POS_REDUCE_READ = 6;
	private static final int POS_REDUCE_UNCOMPRESS = 7;
	private static final int POS_REDUCE_REDUCE = 8;
	private static final int POS_REDUCE_WRITE = 9;
	private static final int POS_REDUCE_COMPRESS = 10;
	private static final int POS_REDUCE_KEY_BYTE_COUNT = 11;
	private static final int POS_REDUCE_VALUE_BYTE_COUNT = 12;
	private static final int POS_REDUCE_MEM = 13;
	private static final int POS_REDUCE_FINAL_WRITE = 14;
	private static final int POS_REDUCE_FINAL_COMPRESS = 15;

	public MRReduceProfileLoader(MRReduceProfile profile, Configuration conf,
			String profileFile) {
		super(profile, conf, profileFile);

		this.shuffleRecords = EMPTY_RECORDS;
		this.reduceRecords = EMPTY_RECORDS;
		this.sortRecords = EMPTY_RECORDS;
		this.mergeRecords = EMPTY_RECORDS;
	}

	protected boolean loadExecutionProfile()
			throws MRTaskProfileLoader.ProfileFormatException {
		if (!getAndValidateProfileRecords()) {
			return false;
		}

		calculateCommonCosts();
		calculateCommonStats();

		calculateStatsAndCosts();
		calculateTimings();

		return true;
	}

	private void calculateStatsAndCosts()
    throws MRTaskProfileLoader.ProfileFormatException
  {
    long hdfsBytesWritten = this.profile.getCounter(MRCounter.HDFS_BYTES_WRITTEN, this.profile.getCounter(MRCounter.S3N_BYTES_WRITTEN, Long.valueOf(0L))).longValue();

    long reduceInputGroups = this.profile.getCounter(MRCounter.REDUCE_INPUT_GROUPS, Long.valueOf(0L)).longValue();

    long reduceInputPairs = this.profile.getCounter(MRCounter.REDUCE_INPUT_RECORDS, Long.valueOf(0L)).longValue();

    long reduceOutputPairs = this.profile.getCounter(MRCounter.REDUCE_OUTPUT_RECORDS, Long.valueOf(0L)).longValue();

    long shuffleBytes = aggregateRecordValues(this.shuffleRecords, 4, 1);

    this.profile.addCounter(MRCounter.REDUCE_SHUFFLE_BYTES, Long.valueOf(shuffleBytes));

    long reduceInputBytes = aggregateRecordValues(this.shuffleRecords, 4, 0);

    this.profile.addCounter(MRCounter.REDUCE_INPUT_BYTES, Long.valueOf(reduceInputBytes));

    String outputFormat = null;
    if (this.conf.get("mapreduce.outputformat.class") != null)
      outputFormat = this.conf.get("mapreduce.outputformat.class");
    else if (this.conf.get("mapred.output.format.class") != null)
      outputFormat = this.conf.get("mapred.output.format.class");
    else {
      outputFormat = this.conf.getBoolean("mapred.reducer.new-api", false) ? "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat" : "org.apache.hadoop.mapred.TextOutputFormat";
    }

    long reduceOutputBytes = 0L;
    if ((outputFormat.equals("org.apache.hadoop.mapreduce.lib.output.TextOutputFormat")) || (outputFormat.equals("org.apache.hadoop.mapred.TextOutputFormat")) || (outputFormat.equals("org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat")) || (outputFormat.equals("org.apache.hadoop.mapred.SequenceFileOutputFormat")) || (outputFormat.equals("org.apache.hadoop.examples.terasort.TeraOutputFormat")) || (outputFormat.equals("org.apache.hadoop.mapreduce.lib.output.StarfishTextOutputFormat")))
    {
      reduceOutputBytes = ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(11)).getValue() + ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(12)).getValue() + 2L * reduceOutputPairs;
    }
    else if (outputFormat.equals("org.apache.hadoop.hbase.mapreduce.TableOutputFormat"))
    {
      reduceOutputBytes = ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(11)).getValue() + ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(12)).getValue();

      this.profile.addCounter(MRCounter.HDFS_BYTES_WRITTEN, Long.valueOf(reduceOutputBytes));
    } else if (outputFormat.equals("org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat"))
    {
      reduceOutputBytes = ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(12)).getValue();
    }
    else
    {
      reduceOutputBytes = isOutputCompressed() ? ()(hdfsBytesWritten / 0.3D) : hdfsBytesWritten;
    }

    if (((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(11)).getValue() == -1L) {
      reduceOutputBytes = hdfsBytesWritten;
    }
    this.profile.addCounter(MRCounter.REDUCE_OUTPUT_BYTES, Long.valueOf(reduceOutputBytes));

    if (this.shuffleRecords.size() > 0) {
      this.profile.addCostFactor(MRCostFactors.NETWORK_COST, Double.valueOf(averageProfileValueDiffRatios(this.shuffleRecords, 4, 2, 3, 1)));
    }

    double comprRatio = 1.0D;
    if (this.conf.getBoolean("mapred.compress.map.output", false) == true) {
      if (this.shuffleRecords.size() > 0) {
        comprRatio = averageRecordValueRatios(this.shuffleRecords, 4, 1, 0);

        this.profile.addStatistic(MRStatistics.INTERM_COMPRESS_RATIO, Double.valueOf(comprRatio));

        this.profile.addCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST, Double.valueOf(averageRecordValueRatios(this.shuffleRecords, 4, 3, 1)));
      }

      long spilledPairs = this.profile.getCounter(MRCounter.SPILLED_RECORDS, Long.valueOf(0L)).longValue();

      if ((spilledPairs != 0L) && (reduceInputPairs != 0L)) {
        double readBytes = spilledPairs * reduceInputBytes / reduceInputPairs;

        long compressTime = ((MRTaskProfileLoader.ProfileRecord)this.sortRecords.get(4)).getValue();

        if (!this.mergeRecords.isEmpty()) {
          compressTime += aggregateRecordValues(this.mergeRecords, 7, 6);
        }

        if ((readBytes != 0.0D) && (compressTime != 0L)) {
          this.profile.addCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST, Double.valueOf(compressTime / readBytes));
        }

      }

    }

    if (reduceInputGroups != 0L) {
      this.profile.addStatistic(MRStatistics.REDUCE_PAIRS_PER_GROUP, Double.valueOf(reduceInputPairs / reduceInputGroups));
    }

    if (reduceInputBytes != 0L) {
      this.profile.addStatistic(MRStatistics.REDUCE_SIZE_SEL, Double.valueOf(reduceOutputBytes / reduceInputBytes));
    }

    if (reduceInputPairs != 0L) {
      this.profile.addStatistic(MRStatistics.REDUCE_PAIRS_SEL, Double.valueOf(reduceOutputPairs / reduceInputPairs));
    }

    if (reduceInputPairs != 0L)
    {
      this.profile.addCostFactor(MRCostFactors.REDUCE_CPU_COST, Double.valueOf((((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(8)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(9)).getValue()) / reduceInputPairs));
    }

    if ((isOutputCompressed()) && (reduceOutputBytes != 0L)) {
      this.profile.addStatistic(MRStatistics.OUT_COMPRESS_RATIO, Double.valueOf(hdfsBytesWritten / reduceOutputBytes));

      this.profile.addCostFactor(MRCostFactors.OUTPUT_COMPRESS_CPU_COST, Double.valueOf((((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(10)).getValue() + ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(15)).getValue()) / reduceOutputBytes));
    }

    if (reduceInputBytes != 0L) {
      this.profile.addCostFactor(MRCostFactors.READ_LOCAL_IO_COST, Double.valueOf(((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(6)).getValue() / (comprRatio * reduceInputBytes)));
    }

    long fileBytesWritten = this.profile.getCounter(MRCounter.FILE_BYTES_WRITTEN, Long.valueOf(0L)).longValue();

    if ((fileBytesWritten != 0L) && (shuffleBytes != 0L) && (((MRTaskProfileLoader.ProfileRecord)this.sortRecords.get(2)).getValue() <= 1L))
    {
      long writeTime = ((MRTaskProfileLoader.ProfileRecord)this.sortRecords.get(1)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.sortRecords.get(4)).getValue();

      if (!this.mergeRecords.isEmpty()) {
        writeTime += aggregateRecordValues(this.mergeRecords, 7, 1) - aggregateRecordValues(this.mergeRecords, 7, 6);
      }

      this.profile.addCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST, Double.valueOf(writeTime / fileBytesWritten));
    }

    double writeTime = ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(9)).getValue() + ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(14)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(10)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(15)).getValue();

    if (hdfsBytesWritten != 0L)
    {
      this.profile.addCostFactor(MRCostFactors.WRITE_HDFS_IO_COST, Double.valueOf(writeTime / hdfsBytesWritten));
    }
    else if (reduceOutputBytes != 0L)
    {
      this.profile.addCostFactor(MRCostFactors.WRITE_HDFS_IO_COST, Double.valueOf(writeTime / reduceOutputBytes));
    }

    if ((this.conf.get("mapreduce.combine.class") != null) || (this.conf.get("mapred.combiner.class") != null))
    {
      long combineInputPairs = this.profile.getCounter(MRCounter.COMBINE_INPUT_RECORDS, Long.valueOf(0L)).longValue();

      long combineOutputPairs = this.profile.getCounter(MRCounter.COMBINE_OUTPUT_RECORDS, Long.valueOf(0L)).longValue();

      if (combineInputPairs != 0L) {
        this.profile.addStatistic(MRStatistics.COMBINE_PAIRS_SEL, Double.valueOf(combineOutputPairs / combineInputPairs));
      }

    }

    long spilledPairs = this.profile.getCounter(MRCounter.SPILLED_RECORDS, Long.valueOf(reduceInputPairs)).longValue();

    if (spilledPairs > 0L) {
      long mergeTime = ((MRTaskProfileLoader.ProfileRecord)this.sortRecords.get(0)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.sortRecords.get(1)).getValue();

      if (!this.mergeRecords.isEmpty()) {
        mergeTime += aggregateRecordValues(this.mergeRecords, 7, 0) - aggregateRecordValues(this.mergeRecords, 7, 1);
      }

      this.profile.addCostFactor(MRCostFactors.MERGE_CPU_COST, Double.valueOf(mergeTime / spilledPairs));
    }

    this.profile.addCostFactor(MRCostFactors.SETUP_CPU_COST, Double.valueOf(((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(1)).getValue()));

    this.profile.addCostFactor(MRCostFactors.CLEANUP_CPU_COST, Double.valueOf(((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(3)).getValue()));

    long startup_mem = ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(0)).getValue();
    long setup_mem = ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(2)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(0)).getValue();

    setup_mem = setup_mem < 0L ? 0L : setup_mem;
    long reduce_mem = ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(13)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(2)).getValue();

    reduce_mem = reduce_mem < 0L ? 0L : reduce_mem;
    long cleanup_mem = ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(4)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.reduceRecords.get(13)).getValue();

    cleanup_mem = cleanup_mem < 0L ? 0L : cleanup_mem;

    this.profile.addStatistic(MRStatistics.STARTUP_MEM, Double.valueOf(startup_mem));
    this.profile.addStatistic(MRStatistics.SETUP_MEM, Double.valueOf(setup_mem));
    this.profile.addStatistic(MRStatistics.CLEANUP_MEM, Double.valueOf(cleanup_mem));
    if (reduceInputPairs != 0L) {
      this.profile.addStatistic(MRStatistics.REDUCE_MEM_PER_RECORD, Double.valueOf(reduce_mem / reduceInputPairs));
    }
    else
      this.profile.addStatistic(MRStatistics.REDUCE_MEM_PER_RECORD, Double.valueOf(reduce_mem));
  }

	private void calculateTimings() {
		double timeShuffle = aggregateRecordValues(this.shuffleRecords, 4, 2);

		if (!this.mergeRecords.isEmpty()) {
			timeShuffle += aggregateRecordValues(this.mergeRecords, 7, 0);
		}

		this.profile.addTiming(MRTaskPhase.SHUFFLE,
				Double.valueOf(timeShuffle / 1000000.0D));

		double timeSort = ((MRTaskProfileLoader.ProfileRecord) this.sortRecords
				.get(0)).getValue();
		this.profile.addTiming(MRTaskPhase.SORT,
				Double.valueOf(timeSort / 1000000.0D));

		this.profile
				.addTiming(
						MRTaskPhase.SETUP,
						Double.valueOf(((MRTaskProfileLoader.ProfileRecord) this.reduceRecords
								.get(1)).getValue() / 1000000.0D));

		this.profile
				.addTiming(
						MRTaskPhase.REDUCE,
						Double.valueOf((((MRTaskProfileLoader.ProfileRecord) this.reduceRecords
								.get(6)).getValue()
								+ ((MRTaskProfileLoader.ProfileRecord) this.reduceRecords
										.get(8)).getValue() - ((MRTaskProfileLoader.ProfileRecord) this.reduceRecords
								.get(9)).getValue()) / 1000000.0D));

		this.profile
				.addTiming(
						MRTaskPhase.WRITE,
						Double.valueOf((((MRTaskProfileLoader.ProfileRecord) this.reduceRecords
								.get(9)).getValue() + ((MRTaskProfileLoader.ProfileRecord) this.reduceRecords
								.get(14)).getValue()) / 1000000.0D));

		this.profile
				.addTiming(
						MRTaskPhase.CLEANUP,
						Double.valueOf(((MRTaskProfileLoader.ProfileRecord) this.reduceRecords
								.get(3)).getValue() / 1000000.0D));
	}

	private boolean getAndValidateProfileRecords()
			throws MRTaskProfileLoader.ProfileFormatException {
		this.shuffleRecords = getProfileRecords(MRTaskProfileLoader.ProfileToken.SHUFFLE);
		if (!validateShuffleRecords(this.shuffleRecords)) {
			return false;
		}
		this.reduceRecords = getProfileRecords(MRTaskProfileLoader.ProfileToken.REDUCE);
		if (!validateReduceRecords(this.reduceRecords)) {
			return false;
		}
		this.sortRecords = getProfileRecords(MRTaskProfileLoader.ProfileToken.SORT);
		if (!validateSortRecords(this.sortRecords)) {
			return false;
		}
		this.mergeRecords = getProfileRecords(MRTaskProfileLoader.ProfileToken.MERGE);

		return validateMergeRecords(this.mergeRecords);
	}

	private boolean isOutputCompressed() {
		return (((MRTaskProfileLoader.ProfileRecord) this.reduceRecords.get(10))
				.getValue() != 0L)
				|| (((MRTaskProfileLoader.ProfileRecord) this.reduceRecords
						.get(15)).getValue() != 0L)
				|| (ProfileUtils.isMROutputCompressionOn(this.conf));
	}

	private boolean validateShuffleRecords(
			List<MRTaskProfileLoader.ProfileRecord> records)
			throws MRTaskProfileLoader.ProfileFormatException {
		if (records.isEmpty()) {
			return false;
		}
		if (records.size() % 4 != 0) {
			throw new MRTaskProfileLoader.ProfileFormatException(
					"Expected groups of 4 records for the SHUFFLE phase for "
							+ this.profile.getTaskId());
		}

		int count = 0;
		for (int i = 0; i < records.size(); i += 4) {
			count += (((MRTaskProfileLoader.ProfileRecord) records.get(i + 0))
					.getProcess().equals("UNCOMPRESS_BYTE_COUNT") ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(i + 1))
					.getProcess().equals("COMPRESS_BYTE_COUNT") ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(i + 2))
					.getProcess().equals("COPY_MAP_DATA") ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(i + 3))
					.getProcess().equals("UNCOMPRESS") ? 0 : 1);
		}

		if (count != 0) {
			throw new MRTaskProfileLoader.ProfileFormatException(
					"Incorrect sequence of records in SHUFFLE phase for "
							+ this.profile.getTaskId());
		}

		for (int i = 0; i < records.size(); i += 4) {
			if (((MRTaskProfileLoader.ProfileRecord) records.get(i + 0))
					.getValue() != 2L)
				continue;
			for (int j = 0; j < 4; j++) {
				records.remove(i);
			}
			i -= 4;
		}

		return true;
	}

	private boolean validateMergeRecords(
			List<MRTaskProfileLoader.ProfileRecord> records)
			throws MRTaskProfileLoader.ProfileFormatException {
		if (records.isEmpty()) {
			return true;
		}
		if (records.size() % 7 != 0) {
			throw new MRTaskProfileLoader.ProfileFormatException(
					"Expected groups of 7 records for the MERGE phase for "
							+ this.profile.getTaskId());
		}

		int count = 0;
		for (int i = 0; i < records.size(); i += 7) {
			count += ((((MRTaskProfileLoader.ProfileRecord) records.get(0))
					.getProcess().equals("MERGE_IN_MEMORY"))
					|| (((MRTaskProfileLoader.ProfileRecord) records.get(0))
							.getProcess().equals("MERGE_TO_DISK")) ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(1))
					.getProcess().equals("READ_WRITE") ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(2))
					.getProcess().equals("READ_WRITE_COUNT") ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(3))
					.getProcess().equals("COMBINE") ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(4))
					.getProcess().equals("WRITE") ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(5))
					.getProcess().equals("UNCOMPRESS") ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(6))
					.getProcess().equals("COMPRESS") ? 0 : 1);
		}

		if (count != 0) {
			throw new MRTaskProfileLoader.ProfileFormatException(
					"Incorrect sequence of records in MERGE phase for "
							+ this.profile.getTaskId());
		}

		return true;
	}

	private boolean validateSortRecords(
			List<MRTaskProfileLoader.ProfileRecord> records)
			throws MRTaskProfileLoader.ProfileFormatException {
		if (records.isEmpty()) {
			return false;
		}
		if (records.size() != 5) {
			throw new MRTaskProfileLoader.ProfileFormatException(
					"Expected 5 records for the SORT phase for "
							+ this.profile.getTaskId());
		}

		int count = 0;
		count += (((MRTaskProfileLoader.ProfileRecord) records.get(0))
				.getProcess().equals("MERGE_MAP_DATA") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(1))
				.getProcess().equals("READ_WRITE") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(2))
				.getProcess().equals("READ_WRITE_COUNT") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(3))
				.getProcess().equals("UNCOMPRESS") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(4))
				.getProcess().equals("COMPRESS") ? 0 : 1);

		if (count != 0) {
			throw new MRTaskProfileLoader.ProfileFormatException(
					"Incorrect sequence of records in SORT phase for "
							+ this.profile.getTaskId());
		}

		return true;
	}

	private boolean validateReduceRecords(
			List<MRTaskProfileLoader.ProfileRecord> records)
			throws MRTaskProfileLoader.ProfileFormatException {
		if (records.isEmpty()) {
			return false;
		}
		if (records.size() != 16) {
			throw new MRTaskProfileLoader.ProfileFormatException(
					"Expected 16 records for the REDUCE phase for "
							+ this.profile.getTaskId());
		}

		int count = 0;
		count += (((MRTaskProfileLoader.ProfileRecord) records.get(0))
				.getProcess().equals("STARTUP_MEM") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(1))
				.getProcess().equals("SETUP") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(2))
				.getProcess().equals("SETUP_MEM") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(3))
				.getProcess().equals("CLEANUP") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(4))
				.getProcess().equals("CLEANUP_MEM") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(5))
				.getProcess().equals("TOTAL_RUN") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(6))
				.getProcess().equals("READ") ? 0 : 1);
		count += (((MRTaskProfileLoader.ProfileRecord) records.get(7))
				.getProcess().equals("UNCOMPRESS") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(8))
				.getProcess().equals("REDUCE") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(9))
				.getProcess().equals("WRITE") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(10))
				.getProcess().equals("COMPRESS") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(11))
				.getProcess().equals("KEY_BYTE_COUNT") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(12))
				.getProcess().equals("VALUE_BYTE_COUNT") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(13))
				.getProcess().equals("REDUCE_MEM") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(14))
				.getProcess().equals("WRITE") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(15))
				.getProcess().equals("COMPRESS") ? 0 : 1);

		if (count != 0) {
			throw new MRTaskProfileLoader.ProfileFormatException(
					"Incorrect sequence of records in REDUCE phase for "
							+ this.profile.getTaskId());
		}

		return true;
	}
}