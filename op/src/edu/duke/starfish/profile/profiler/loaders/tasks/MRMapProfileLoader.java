package edu.duke.starfish.profile.profiler.loaders.tasks;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;
import edu.duke.starfish.profile.utils.GeneralUtils;
import edu.duke.starfish.profile.utils.ProfileUtils;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

public class MRMapProfileLoader extends MRTaskProfileLoader {
	private List<MRTaskProfileLoader.ProfileRecord> mapRecords;
	private List<MRTaskProfileLoader.ProfileRecord> spillRecords;
	private List<MRTaskProfileLoader.ProfileRecord> mergeRecords;
	private static final int NUM_MAP_PHASES = 21;
	private static final int POS_MAP_INPUT = 0;
	private static final int POS_MAP_STARTUP_MEM = 1;
	private static final int POS_MAP_SETUP = 2;
	private static final int POS_MAP_SETUP_MEM = 3;
	private static final int POS_MAP_CLEANUP = 4;
	private static final int POS_MAP_CLEANUP_MEM = 5;
	private static final int POS_MAP_TOTAL_RUN = 6;
	private static final int POS_MAP_READ = 7;
	private static final int POS_MAP_UNCOMPRESS = 8;
	private static final int POS_MAP_INPUT_K_BYTE_COUNT = 9;
	private static final int POS_MAP_INPUT_V_BYTE_COUNT = 10;
	private static final int POS_MAP_MAP = 11;
	private static final int POS_MAP_WRITE = 12;
	private static final int POS_MAP_COMPRESS = 13;
	private static final int POS_MAP_PARTITION_OUTPUT = 14;
	private static final int POS_MAP_SERIALIZE_OUTPUT = 15;
	private static final int POS_MAP_MEM = 16;
	private static final int POS_MAP_DIR_WRITE = 17;
	private static final int POS_MAP_DIR_COMPRESS = 18;
	private static final int POS_MAP_OUTPUT_K_BYTE_COUNT = 19;
	private static final int POS_MAP_OUTPUT_V_BYTE_COUNT = 20;
	private static final int NUM_SPILL_PHASES = 8;
	private static final int POS_SPILL_SORT_AND_SPILL = 0;
	private static final int POS_SPILL_QUICK_SORT = 1;
	private static final int POS_SPILL_SORT_COUNT = 2;
	private static final int POS_SPILL_COMBINE = 3;
	private static final int POS_SPILL_WRITE = 4;
	private static final int POS_SPILL_COMPRESS = 5;
	private static final int POS_SPILL_UNCOMPRESS_BYTE_COUNT = 6;
	private static final int POS_SPILL_COMPRESS_BYTE_COUNT = 7;
	private static final int NUM_MERGE_PHASES = 5;
	private static final int POS_MERGE_TOTAL_MERGE = 0;
	private static final int POS_MERGE_READ_WRITE = 1;
	private static final int POS_MERGE_READ_WRITE_COUNT = 2;
	private static final int POS_MERGE_UNCOMPRESS = 3;
	private static final int POS_MERGE_COMPRESS = 4;

	public MRMapProfileLoader(MRMapProfile profile, Configuration conf,
			String profileFile) {
		super(profile, conf, profileFile);

		this.mapRecords = EMPTY_RECORDS;
		this.spillRecords = EMPTY_RECORDS;
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
    long hdfsBytesRead = this.profile.getCounter(MRCounter.HDFS_BYTES_READ, this.profile.getCounter(MRCounter.S3N_BYTES_READ, Long.valueOf(0L))).longValue();

    long hdfsBytesWritten = this.profile.getCounter(MRCounter.HDFS_BYTES_WRITTEN, this.profile.getCounter(MRCounter.S3N_BYTES_WRITTEN, Long.valueOf(0L))).longValue();

    long mapInputBytes = this.profile.getCounter(MRCounter.MAP_INPUT_BYTES, Long.valueOf(0L)).longValue();
    long mapOutputBytes = this.profile.getCounter(MRCounter.MAP_OUTPUT_BYTES, Long.valueOf(0L)).longValue();

    long mapInputPairs = this.profile.getCounter(MRCounter.MAP_INPUT_RECORDS, Long.valueOf(1L)).longValue();

    long mapOutputPairs = this.profile.getCounter(MRCounter.MAP_OUTPUT_RECORDS, Long.valueOf(0L)).longValue();

    if ((mapInputBytes == 0L) && (((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(9)).getValue() == -1L))
    {
      mapInputBytes = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(10)).getValue();
    }
    else if (mapInputBytes == 0L)
    {
      String inputFormat = null;
      if (this.conf.get("mapreduce.inputformat.class") != null)
        inputFormat = this.conf.get("mapreduce.inputformat.class");
      else if (this.conf.get("mapred.input.format.class") != null)
        inputFormat = this.conf.get("mapred.input.format.class");
      else {
        inputFormat = this.conf.getBoolean("mapred.mapper.new-api", false) ? "org.apache.hadoop.mapreduce.lib.input.TextInputFormat" : "org.apache.hadoop.mapred.TextInputFormat";
      }

      if ((inputFormat.equals("org.apache.hadoop.mapreduce.lib.input.TextInputFormat")) || (inputFormat.equals("org.apache.hadoop.mapred.TextInputFormat")) || (inputFormat.equals("org.apache.hadoop.mapreduce.lib.input.StarfishTextInputFormat")) || (inputFormat.equals("org.apache.hadoop.mapreduce.lib.input.WholeFileInputFormat")))
      {
        mapInputBytes = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(10)).getValue() + mapInputPairs;
      }
      else if ((inputFormat.equals("org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat")) || (inputFormat.equals("org.apache.hadoop.mapred.SequenceFileInputFormat")) || (inputFormat.equals("org.apache.hadoop.examples.terasort.TeraInputFormat")) || (inputFormat.equals("org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat")) || (inputFormat.equals("org.apache.hadoop.mapreduce.lib.input.KeyValueTextPairInputFormat")) || (inputFormat.equals("org.apache.hadoop.mapreduce.lib.input.WholeFileTextPairInputFormat")))
      {
        mapInputBytes = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(9)).getValue() + ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(10)).getValue() + 2L * mapInputPairs;
      }
      else if (inputFormat.equals("org.apache.hadoop.hbase.mapreduce.TableInputFormat"))
      {
        mapInputBytes = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(9)).getValue() + ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(10)).getValue();

        this.profile.addCounter(MRCounter.HDFS_BYTES_READ, Long.valueOf(mapInputBytes));
      }
      else if (inputFormat.equals("org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat"))
      {
        mapInputBytes = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(10)).getValue();
      }
      else
      {
        mapInputBytes = isInputCompressed() ? ()(hdfsBytesRead / 0.3D) : hdfsBytesRead;
      }
    }

    this.profile.addCounter(MRCounter.MAP_INPUT_BYTES, Long.valueOf(mapInputBytes));

    if (mapOutputBytes == 0L)
    {
      String outputFormat = null;
      if (this.conf.get("mapreduce.outputformat.class") != null)
        outputFormat = this.conf.get("mapreduce.outputformat.class");
      else if (this.conf.get("mapred.output.format.class") != null)
        outputFormat = this.conf.get("mapred.output.format.class");
      else {
        outputFormat = this.conf.getBoolean("mapred.mapper.new-api", false) ? "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat" : "org.apache.hadoop.mapred.TextOutputFormat";
      }

      if ((outputFormat.equals("org.apache.hadoop.mapreduce.lib.output.TextOutputFormat")) || (outputFormat.equals("org.apache.hadoop.mapred.TextOutputFormat")) || (outputFormat.equals("org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat")) || (outputFormat.equals("org.apache.hadoop.mapred.SequenceFileOutputFormat")) || (outputFormat.equals("org.apache.hadoop.examples.terasort.TeraOutputFormat")))
      {
        mapOutputBytes = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(19)).getValue() + ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(20)).getValue() + 2L * mapOutputPairs;
      }
      else if (outputFormat.equals("org.apache.hadoop.hbase.mapreduce.TableOutputFormat"))
      {
        mapOutputBytes = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(19)).getValue() + ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(20)).getValue();

        this.profile.addCounter(MRCounter.HDFS_BYTES_WRITTEN, Long.valueOf(mapOutputBytes));
      }
      else if (outputFormat.equals("org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat"))
      {
        mapOutputBytes = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(20)).getValue();
      }
      else
      {
        mapOutputBytes = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(13)).getValue() == 0L ? hdfsBytesWritten : ()(hdfsBytesWritten / 0.3D);
      }

      this.profile.addCounter(MRCounter.MAP_OUTPUT_BYTES, Long.valueOf(mapOutputBytes));
    }

    if (mapOutputBytes == 0L) {
      this.profile.addCounter(MRCounter.MAP_OUTPUT_MATERIALIZED_BYTES, Long.valueOf(0L));
    }

    if (mapInputBytes != 0L) {
      this.profile.addStatistic(MRStatistics.MAP_SIZE_SEL, Double.valueOf(mapOutputBytes / mapInputBytes));
    }

    if (mapInputPairs != 0L) {
      this.profile.addStatistic(MRStatistics.MAP_PAIRS_SEL, Double.valueOf(mapOutputPairs / mapInputPairs));

      this.profile.addStatistic(MRStatistics.INPUT_PAIR_WIDTH, Double.valueOf(mapInputBytes / mapInputPairs));
    }

    if (mapInputPairs != 0L)
    {
      this.profile.addCostFactor(MRCostFactors.MAP_CPU_COST, Double.valueOf((((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(11)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(12)).getValue()) / mapInputPairs));
    }

    double readTime = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(7)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(8)).getValue();

    if (hdfsBytesRead != 0L)
    {
      this.profile.addCostFactor(MRCostFactors.READ_HDFS_IO_COST, Double.valueOf(readTime / hdfsBytesRead));
    }
    else if (mapInputBytes != 0L)
    {
      this.profile.addCostFactor(MRCostFactors.READ_HDFS_IO_COST, Double.valueOf(readTime / mapInputBytes));
    }

    if ((isInputCompressed()) || ((hdfsBytesRead != 0L) && (mapInputBytes > 1.1D * hdfsBytesRead)))
    {
      if (mapInputBytes != 0L) {
        this.profile.addStatistic(MRStatistics.INPUT_COMPRESS_RATIO, Double.valueOf(hdfsBytesRead / mapInputBytes));
      }

      if (hdfsBytesRead != 0L)
      {
        this.profile.addCostFactor(MRCostFactors.INPUT_UNCOMPRESS_CPU_COST, Double.valueOf(((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(8)).getValue() / hdfsBytesRead));
      }

    }

    int numReducers = this.conf.getInt("mapred.reduce.tasks", 1);
    if (numReducers == 0)
    {
      if ((ProfileUtils.isMROutputCompressionOn(this.conf)) && 
        (mapOutputBytes != 0L)) {
        this.profile.addStatistic(MRStatistics.OUT_COMPRESS_RATIO, Double.valueOf(hdfsBytesWritten / mapOutputBytes));

        this.profile.addCostFactor(MRCostFactors.OUTPUT_COMPRESS_CPU_COST, Double.valueOf((((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(13)).getValue() + ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(18)).getValue()) / mapOutputBytes));
      }

      double writeTime = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(12)).getValue() + ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(17)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(13)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(18)).getValue();

      if (hdfsBytesWritten != 0L)
      {
        this.profile.addCostFactor(MRCostFactors.WRITE_HDFS_IO_COST, Double.valueOf(writeTime / hdfsBytesWritten));
      }
      else if (mapOutputBytes != 0L)
      {
        this.profile.addCostFactor(MRCostFactors.WRITE_HDFS_IO_COST, Double.valueOf(writeTime / mapOutputBytes));
      }

    }
    else
    {
      if (!this.spillRecords.isEmpty()) {
        this.profile.addCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST, Double.valueOf(averageProfileValueDiffRatios(this.spillRecords, 8, 4, 5, 7)));
      }

      if ((this.conf.get("mapreduce.combine.class") != null) || (this.conf.get("mapred.combiner.class") != null))
      {
        long combineInputPairs = this.profile.getCounter(MRCounter.COMBINE_INPUT_RECORDS, Long.valueOf(1L)).longValue();

        long combineOutputPairs = this.profile.getCounter(MRCounter.COMBINE_OUTPUT_RECORDS, Long.valueOf(1L)).longValue();

        if (mapOutputBytes != 0L) {
          this.profile.addStatistic(MRStatistics.COMBINE_SIZE_SEL, Double.valueOf(aggregateRecordValues(this.spillRecords, 8, 6) / mapOutputBytes));
        }

        if (combineInputPairs != 0L) {
          this.profile.addStatistic(MRStatistics.COMBINE_PAIRS_SEL, Double.valueOf(combineOutputPairs / combineInputPairs));
        }

        if (!this.spillRecords.isEmpty()) {
          this.profile.addCostFactor(MRCostFactors.COMBINE_CPU_COST, Double.valueOf(averageProfileValueDiffRatios(this.spillRecords, 8, 3, 4, 2)));
        }

      }

      if (mapOutputPairs != 0L)
      {
        this.profile.addCostFactor(MRCostFactors.PARTITION_CPU_COST, Double.valueOf(((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(14)).getValue() / mapOutputPairs));
      }

      if (mapOutputPairs != 0L)
      {
        this.profile.addCostFactor(MRCostFactors.SERDE_CPU_COST, Double.valueOf(((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(15)).getValue() / mapOutputPairs));
      }

      if (!this.spillRecords.isEmpty()) {
        this.profile.addCostFactor(MRCostFactors.SORT_CPU_COST, Double.valueOf(averageSortCostInSpills(this.spillRecords, 1, 2, numReducers)));
      }

      long combineOutputPairs = this.profile.getCounter(MRCounter.COMBINE_OUTPUT_RECORDS, Long.valueOf(0L)).longValue();

      long spilledPairs = this.profile.getCounter(MRCounter.SPILLED_RECORDS, Long.valueOf(mapOutputPairs)).longValue();

      long outputPairs = combineOutputPairs == 0L ? mapOutputPairs : combineOutputPairs;

      long numMergedPairs = spilledPairs - outputPairs;

      if (numMergedPairs > 0L) {
        this.profile.addCostFactor(MRCostFactors.MERGE_CPU_COST, Double.valueOf((((MRTaskProfileLoader.ProfileRecord)this.mergeRecords.get(0)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.mergeRecords.get(1)).getValue()) / numMergedPairs));
      }

      if ((this.conf.getBoolean("mapred.compress.map.output", false) == true) && (!this.spillRecords.isEmpty()))
      {
        this.profile.addStatistic(MRStatistics.INTERM_COMPRESS_RATIO, Double.valueOf(averageRecordValueRatios(this.spillRecords, 8, 7, 6)));

        this.profile.addCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST, Double.valueOf(averageRecordValueRatios(this.spillRecords, 8, 5, 6)));

        double readBytes = (spilledPairs / outputPairs - 1.0D) * aggregateRecordValues(this.spillRecords, 8, 7);

        if ((outputPairs != 0L) && (readBytes > 0.0D) && (((MRTaskProfileLoader.ProfileRecord)this.mergeRecords.get(3)).getValue() != 0L))
        {
          this.profile.addCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST, Double.valueOf(((MRTaskProfileLoader.ProfileRecord)this.mergeRecords.get(3)).getValue() / readBytes));
        }

      }

      this.profile.addCounter(MRCounter.MAP_NUM_SPILLS, Long.valueOf(this.spillRecords.size() / 8L));

      this.profile.addCounter(MRCounter.MAP_NUM_SPILL_MERGES, Long.valueOf(((MRTaskProfileLoader.ProfileRecord)this.mergeRecords.get(2)).getValue() / numReducers));

      if (!this.spillRecords.isEmpty()) {
        this.profile.addCounter(MRCounter.MAP_RECS_PER_BUFF_SPILL, Long.valueOf(()averageRecordValues(this.spillRecords, 8, 2)));

        this.profile.addCounter(MRCounter.MAP_SPILL_SIZE, Long.valueOf(z)averageRecordValues(this.spillRecords, 8, 7)));
      }

    }

    this.profile.addCostFactor(MRCostFactors.SETUP_CPU_COST, Double.valueOf(((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(2)).getValue()));

    this.profile.addCostFactor(MRCostFactors.CLEANUP_CPU_COST, Double.valueOf(((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(4)).getValue()));

    long startup_mem = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(1)).getValue();
    long setup_mem = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(3)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(1)).getValue();

    setup_mem = setup_mem < 0L ? 0L : setup_mem;
    long map_mem = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(16)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(3)).getValue();

    map_mem = map_mem < 0L ? 0L : map_mem;
    long cleanup_mem = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(5)).getValue() - ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(16)).getValue();

    cleanup_mem = cleanup_mem < 0L ? 0L : cleanup_mem;

    int sortmb = this.conf.getInt("io.sort.mb", 100) << 20;
    startup_mem = startup_mem > sortmb ? startup_mem - sortmb : 0L;

    this.profile.addStatistic(MRStatistics.STARTUP_MEM, Double.valueOf(startup_mem));
    this.profile.addStatistic(MRStatistics.SETUP_MEM, Double.valueOf(setup_mem));
    this.profile.addStatistic(MRStatistics.CLEANUP_MEM, Double.valueOf(cleanup_mem));
    if (mapInputPairs != 0L) {
      this.profile.addStatistic(MRStatistics.MAP_MEM_PER_RECORD, Double.valueOf(map_mem / mapInputPairs));
    }
    else {
      this.profile.addStatistic(MRStatistics.MAP_MEM_PER_RECORD, Double.valueOf(map_mem));
    }

    if (this.profile.containsStatistic(MRStatistics.MAX_MEMORY)) {
      double max = this.profile.getStatistic(MRStatistics.MAX_MEMORY).doubleValue();
      max = max > sortmb ? max - sortmb : 0.0D;
      this.profile.addStatistic(MRStatistics.MAX_MEMORY, Double.valueOf(max));
    }
    if (this.profile.containsStatistic(MRStatistics.MIN_MEMORY)) {
      double min = this.profile.getStatistic(MRStatistics.MIN_MEMORY).doubleValue();
      min = min > sortmb ? min - sortmb : 0.0D;
      this.profile.addStatistic(MRStatistics.MIN_MEMORY, Double.valueOf(min));
    }
    if (this.profile.containsStatistic(MRStatistics.AVG_MEMORY)) {
      double avg = this.profile.getStatistic(MRStatistics.AVG_MEMORY).doubleValue();
      avg = avg > sortmb ? avg - sortmb : 0.0D;
      this.profile.addStatistic(MRStatistics.AVG_MEMORY, Double.valueOf(avg));
    }

    String[] jobInputs = ProfileUtils.getInputDirs(this.conf);
    String mapInput = ((MRTaskProfileLoader.ProfileRecord)this.mapRecords.get(0)).getProcess();
    int index = GeneralUtils.getIndexInPathArray(jobInputs, mapInput);
    ((MRMapProfile)this.profile).setInputIndex(index == -1 ? 0 : index);
  }

	private void calculateTimings() {
		this.profile.addTiming(MRTaskPhase.SETUP, Double
				.valueOf(((MRTaskProfileLoader.ProfileRecord) this.mapRecords
						.get(2)).getValue() / 1000000.0D));

		this.profile.addTiming(MRTaskPhase.READ, Double
				.valueOf(((MRTaskProfileLoader.ProfileRecord) this.mapRecords
						.get(7)).getValue() / 1000000.0D));

		this.profile
				.addTiming(
						MRTaskPhase.MAP,
						Double.valueOf((((MRTaskProfileLoader.ProfileRecord) this.mapRecords
								.get(11)).getValue() - ((MRTaskProfileLoader.ProfileRecord) this.mapRecords
								.get(12)).getValue()) / 1000000.0D));

		this.profile.addTiming(MRTaskPhase.CLEANUP, Double
				.valueOf(((MRTaskProfileLoader.ProfileRecord) this.mapRecords
						.get(4)).getValue() / 1000000.0D));

		int numReducers = this.conf.getInt("mapred.reduce.tasks", 1);
		if (numReducers == 0) {
			this.profile
					.addTiming(
							MRTaskPhase.WRITE,
							Double.valueOf((((MRTaskProfileLoader.ProfileRecord) this.mapRecords
									.get(12)).getValue() + ((MRTaskProfileLoader.ProfileRecord) this.mapRecords
									.get(17)).getValue()) / 1000000.0D));
		} else {
			this.profile
					.addTiming(
							MRTaskPhase.COLLECT,
							Double.valueOf((((MRTaskProfileLoader.ProfileRecord) this.mapRecords
									.get(14)).getValue() + ((MRTaskProfileLoader.ProfileRecord) this.mapRecords
									.get(15)).getValue()) / 1000000.0D));

			this.profile.addTiming(MRTaskPhase.SPILL,
					Double.valueOf(aggregateRecordValues(this.spillRecords, 8,
							0) / 1000000.0D));

			if (!this.mergeRecords.isEmpty()) {
				this.profile
						.addTiming(
								MRTaskPhase.MERGE,
								Double.valueOf(((MRTaskProfileLoader.ProfileRecord) this.mergeRecords
										.get(0)).getValue() / 1000000.0D));
			} else {
				this.profile.addTiming(MRTaskPhase.MERGE, Double.valueOf(0.0D));
			}
		}
	}

	private double averageSortCostInSpills(
			List<MRTaskProfileLoader.ProfileRecord> records, int sortPos,
			int countPos, int numReducers) {
		double sumCosts = 0.0D;
		int numSpills = 0;
		double numRecsPerRed = 1.0D;

		for (int i = 0; i < records.size(); i += 8) {
			numRecsPerRed = ((MRTaskProfileLoader.ProfileRecord) records.get(i
					+ countPos)).getValue()
					/ numReducers;

			sumCosts += ((MRTaskProfileLoader.ProfileRecord) records.get(i
					+ sortPos)).getValue()
					* Math.log(2.0D)
					/ (((MRTaskProfileLoader.ProfileRecord) records.get(i
							+ countPos)).getValue() * Math
								.log(numRecsPerRed < 2.0D ? 2.0D
										: numRecsPerRed));

			numSpills++;
		}

		return sumCosts / numSpills;
	}

	private boolean getAndValidateProfileRecords()
			throws MRTaskProfileLoader.ProfileFormatException {
		boolean mapOnly = this.conf.getInt("mapred.reduce.tasks", 1) == 0;

		this.mapRecords = getProfileRecords(MRTaskProfileLoader.ProfileToken.MAP);
		if (!validateMapRecords(this.mapRecords)) {
			return false;
		}
		if (!mapOnly) {
			this.spillRecords = getProfileRecords(MRTaskProfileLoader.ProfileToken.SPILL);
			if (!validateSpillRecords(this.spillRecords)) {
				return false;
			}
			this.mergeRecords = getProfileRecords(MRTaskProfileLoader.ProfileToken.MERGE);
			if (!validateMergeRecords(this.mergeRecords)) {
				return false;
			}
		}
		return true;
	}

	private boolean isInputCompressed() {
		if (((MRTaskProfileLoader.ProfileRecord) this.mapRecords.get(8))
				.getValue() != 0L) {
			return true;
		}
		String mapInput = ((MRTaskProfileLoader.ProfileRecord) this.mapRecords
				.get(0)).getProcess();
		if (GeneralUtils.hasCompressionExtension(mapInput)) {
			return true;
		}
		if (ProfileUtils.isPigTempPath(this.conf, mapInput)) {
			return this.conf.getBoolean("pig.tmpfilecompression", false);
		}
		return false;
	}

	private boolean validateMapRecords(
			List<MRTaskProfileLoader.ProfileRecord> records)
			throws MRTaskProfileLoader.ProfileFormatException {
		if (records.isEmpty()) {
			return false;
		}
		if (records.size() != 21) {
			throw new MRTaskProfileLoader.ProfileFormatException(
					"Expected 21 records for the MAP phase for "
							+ this.profile.getTaskId());
		}

		int count = 0;
		count += (((MRTaskProfileLoader.ProfileRecord) records.get(1))
				.getProcess().equals("STARTUP_MEM") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(2))
				.getProcess().equals("SETUP") ? 0 : 1);
		count += (((MRTaskProfileLoader.ProfileRecord) records.get(3))
				.getProcess().equals("SETUP_MEM") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(4))
				.getProcess().equals("CLEANUP") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(5))
				.getProcess().equals("CLEANUP_MEM") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(6))
				.getProcess().equals("TOTAL_RUN") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(7))
				.getProcess().equals("READ") ? 0 : 1);
		count += (((MRTaskProfileLoader.ProfileRecord) records.get(8))
				.getProcess().equals("UNCOMPRESS") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(9))
				.getProcess().equals("KEY_BYTE_COUNT") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(10))
				.getProcess().equals("VALUE_BYTE_COUNT") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(11))
				.getProcess().equals("MAP") ? 0 : 1);
		count += (((MRTaskProfileLoader.ProfileRecord) records.get(12))
				.getProcess().equals("WRITE") ? 0 : 1);
		count += (((MRTaskProfileLoader.ProfileRecord) records.get(13))
				.getProcess().equals("COMPRESS") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(14))
				.getProcess().equals("PARTITION_OUTPUT") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(15))
				.getProcess().equals("SERIALIZE_OUTPUT") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(16))
				.getProcess().equals("MAP_MEM") ? 0 : 1);
		count += (((MRTaskProfileLoader.ProfileRecord) records.get(17))
				.getProcess().equals("WRITE") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(18))
				.getProcess().equals("COMPRESS") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(19))
				.getProcess().equals("KEY_BYTE_COUNT") ? 0 : 1);

		count += (((MRTaskProfileLoader.ProfileRecord) records.get(20))
				.getProcess().equals("VALUE_BYTE_COUNT") ? 0 : 1);

		if (count != 0) {
			throw new MRTaskProfileLoader.ProfileFormatException(
					"Incorrect sequence of records in MAP phase for "
							+ this.profile.getTaskId());
		}

		return true;
	}

	private boolean validateSpillRecords(
			List<MRTaskProfileLoader.ProfileRecord> records)
			throws MRTaskProfileLoader.ProfileFormatException {
		if (records.isEmpty()) {
			return true;
		}
		if (records.size() % 8 != 0) {
			throw new MRTaskProfileLoader.ProfileFormatException(
					"Expected groups of 8 records for the SPILL phase for "
							+ this.profile.getTaskId());
		}

		int count = 0;
		for (int i = 0; i < records.size(); i += 8) {
			count += (((MRTaskProfileLoader.ProfileRecord) records.get(i + 0))
					.getProcess().equals("SORT_AND_SPILL") ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(i + 1))
					.getProcess().equals("QUICK_SORT") ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(i + 2))
					.getProcess().equals("SORT_COUNT") ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(i + 3))
					.getProcess().equals("COMBINE") ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(i + 4))
					.getProcess().equals("WRITE") ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(i + 5))
					.getProcess().equals("COMPRESS") ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(i + 6))
					.getProcess().equals("UNCOMPRESS_BYTE_COUNT") ? 0 : 1);

			count += (((MRTaskProfileLoader.ProfileRecord) records.get(i + 7))
					.getProcess().equals("COMPRESS_BYTE_COUNT") ? 0 : 1);
		}

		if (count != 0) {
			throw new MRTaskProfileLoader.ProfileFormatException(
					"Incorrect sequence of records in SPILL phase for "
							+ this.profile.getTaskId());
		}

		return true;
	}

	private boolean validateMergeRecords(
			List<MRTaskProfileLoader.ProfileRecord> records)
			throws MRTaskProfileLoader.ProfileFormatException {
		if (records.isEmpty()) {
			return false;
		}
		if (records.size() != 5) {
			throw new MRTaskProfileLoader.ProfileFormatException(
					"Expected 5 records for the MERGE phase for "
							+ this.profile.getTaskId());
		}

		int count = 0;
		count += (((MRTaskProfileLoader.ProfileRecord) records.get(0))
				.getProcess().equals("TOTAL_MERGE") ? 0 : 1);

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
					"Incorrect sequence of records in MERGE phase for "
							+ this.profile.getTaskId());
		}

		return true;
	}
}