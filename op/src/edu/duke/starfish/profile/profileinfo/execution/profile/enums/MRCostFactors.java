package edu.duke.starfish.profile.profileinfo.execution.profile.enums;

public enum MRCostFactors {
	READ_HDFS_IO_COST, WRITE_HDFS_IO_COST, READ_LOCAL_IO_COST, WRITE_LOCAL_IO_COST,

	NETWORK_COST,

	MAP_CPU_COST, REDUCE_CPU_COST, COMBINE_CPU_COST,

	PARTITION_CPU_COST, SERDE_CPU_COST, SORT_CPU_COST, MERGE_CPU_COST,

	INPUT_UNCOMPRESS_CPU_COST, INTERM_UNCOMPRESS_CPU_COST, INTERM_COMPRESS_CPU_COST, OUTPUT_COMPRESS_CPU_COST,

	SETUP_CPU_COST, CLEANUP_CPU_COST;

	public String getDescription()
  {
    switch (1.$SwitchMap$edu$duke$starfish$profile$profileinfo$execution$profile$enums$MRCostFactors[ordinal()]) {
    case 1:
      return "I/O cost for reading from HDFS";
    case 2:
      return "I/O cost for writing to HDFS";
    case 3:
      return "I/O cost for reading from local disk";
    case 4:
      return "I/O cost for writing to local disk";
    case 5:
      return "Cost for network transfers";
    case 6:
      return "CPU cost for executing the Mapper";
    case 7:
      return "CPU cost for executing the Reducer";
    case 8:
      return "CPU cost for executing the Combiner";
    case 9:
      return "CPU cost for partitioning";
    case 10:
      return "CPU cost for serializing/deserializing";
    case 11:
      return "CPU cost for sorting";
    case 12:
      return "CPU cost for merging";
    case 13:
      return "CPU cost for uncompressing the input";
    case 14:
      return "CPU cost for uncompressing map output";
    case 15:
      return "CPU cost for compressing map output";
    case 16:
      return "CPU cost for compressing the output";
    case 17:
      return "CPU cost for task setup";
    case 18:
      return "CPU cost for task cleanup";
    }
    return toString();
  }
}