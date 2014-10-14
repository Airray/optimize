package edu.duke.starfish.profile.profileinfo.execution.profile.enums;

public enum MRStatistics {
	INPUT_PAIR_WIDTH, REDUCE_PAIRS_PER_GROUP,

	MAP_SIZE_SEL, MAP_PAIRS_SEL, REDUCE_SIZE_SEL, REDUCE_PAIRS_SEL, COMBINE_SIZE_SEL, COMBINE_PAIRS_SEL,

	INPUT_COMPRESS_RATIO, INTERM_COMPRESS_RATIO, OUT_COMPRESS_RATIO,

	STARTUP_MEM, SETUP_MEM, MAP_MEM_PER_RECORD, REDUCE_MEM_PER_RECORD, CLEANUP_MEM,

	MAX_MEMORY, AVG_MEMORY, MIN_MEMORY;

	public String getDescription()
  {
    switch (1.$SwitchMap$edu$duke$starfish$profile$profileinfo$execution$profile$enums$MRStatistics[ordinal()]) {
    case 1:
      return "Average width of input key-value pairs";
    case 2:
      return "Number of records per reducer's group";
    case 3:
      return "Map selectivity in terms of size";
    case 4:
      return "Map selectivity in terms of records";
    case 5:
      return "Reducer selectivity in terms of size";
    case 6:
      return "Reducer selectivity in terms of records";
    case 7:
      return "Combiner selectivity in terms of size";
    case 8:
      return "Combiner selectivity in terms of records";
    case 9:
      return "Input data compression ratio";
    case 10:
      return "Map output data compression ratio";
    case 11:
      return "Output data compression ratio";
    case 12:
      return "Startup memory per task";
    case 13:
      return "Setup memory per task";
    case 14:
      return "Memory per map's record";
    case 15:
      return "Memory per reducer's record";
    case 16:
      return "Cleanup memory per task";
    case 17:
      return "Maximum memory usage during the lifetime of a task";
    case 18:
      return "Average memory usage during the lifetime of a task";
    case 19:
      return "Minimum memory usage during the lifetime of a task";
    }

    return toString();
  }
}