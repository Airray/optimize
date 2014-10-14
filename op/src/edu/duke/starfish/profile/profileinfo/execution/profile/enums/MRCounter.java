package edu.duke.starfish.profile.profileinfo.execution.profile.enums;

public enum MRCounter {
	MAP_TASKS, REDUCE_TASKS,

	MAP_INPUT_RECORDS, MAP_INPUT_BYTES, MAP_OUTPUT_RECORDS, MAP_OUTPUT_BYTES, MAP_OUTPUT_MATERIALIZED_BYTES, MAP_SKIPPED_RECORDS, MAP_NUM_SPILLS, MAP_NUM_SPILL_MERGES, MAP_RECS_PER_BUFF_SPILL, MAP_BUFF_SPILL_SIZE, MAP_RECORDS_PER_SPILL, MAP_SPILL_SIZE, MAP_MAX_UNIQUE_GROUPS,

	REDUCE_SHUFFLE_BYTES, REDUCE_INPUT_GROUPS, REDUCE_INPUT_RECORDS, REDUCE_INPUT_BYTES, REDUCE_OUTPUT_RECORDS, REDUCE_OUTPUT_BYTES, REDUCE_SKIPPED_RECORDS, REDUCE_SKIPPED_GROUPS,

	COMBINE_INPUT_RECORDS, COMBINE_OUTPUT_RECORDS,

	SPILLED_RECORDS,

	FILE_BYTES_READ, FILE_BYTES_WRITTEN, SPLIT_RAW_BYTES, HDFS_BYTES_READ, HDFS_BYTES_WRITTEN, S3N_BYTES_READ, S3N_BYTES_WRITTEN,

	PIG_PROACTIVE_SPILL_BYTES, PIG_ROACTIVE_SPILL_RECORDS, PIG_ROACTIVE_SPILL_BAGS,

	PIG_MANAGER_SPILL_BYTES, PIG_MANAGER_SPILL_RECORDS, PIG_MANAGER_SPILL_BAGS, PIG_MANAGER_SPILL_NOTIFICATION_COUNT, PIG_MANAGER_SPILL_ACTUAL_COUNT;

	public String getDescription()
  {
    switch (1.$SwitchMap$edu$duke$starfish$profile$profileinfo$execution$profile$enums$MRCounter[ordinal()]) {
    case 1:
      return "Number of map tasks in the job";
    case 2:
      return "Number of reduce tasks in the job";
    case 3:
      return "Map input records";
    case 4:
      return "Map input bytes";
    case 5:
      return "Map output records";
    case 6:
      return "Map output bytes";
    case 7:
      return "Map output materialized bytes";
    case 8:
      return "Map skipped records";
    case 9:
      return "Number of spills";
    case 10:
      return "Number of merge rounds";
    case 11:
      return "Number of records in buffer per spill";
    case 12:
      return "Buffer size (bytes) per spill";
    case 13:
      return "Number of records in spill file";
    case 14:
      return "Spill file size (bytes)";
    case 15:
      return "Maximum number of unique groups";
    case 16:
      return "Shuffle size (bytes)";
    case 17:
      return "Reduce input groups (unique keys)";
    case 18:
      return "Reduce input records";
    case 19:
      return "Reduce input bytes";
    case 20:
      return "Reduce output records";
    case 21:
      return "Reduce output bytes";
    case 22:
      return "Reduce skipped records";
    case 23:
      return "Reduce skipped groups";
    case 24:
      return "Combine input records";
    case 25:
      return "Combine output records";
    case 26:
      return "Total spilled records";
    case 27:
      return "Bytes read from local file system";
    case 28:
      return "Bytes written to local file system";
    case 29:
      return "Split metadata bytes read from HDFS";
    case 30:
      return "Bytes read from HDFS";
    case 31:
      return "Bytes written to HDFS";
    case 32:
      return "Bytes read from S3";
    case 33:
      return "Bytes written to S3";
    case 34:
      return "Bytes spilled proactively by databags";
    case 35:
      return "Number of records Spilled proactively by databags";
    case 36:
      return "Number of bags that have proactively spilled";
    case 37:
      return "Bytes spilled by memory manager";
    case 38:
      return "Number of records Spilled by memory manager";
    case 39:
      return "Number of bags spilled by memory manager";
    case 40:
      return "Number of times the notification handler was triggered";
    case 41:
      return "Number of times the notification handler spilled when triggered";
    }

    return toString();
  }

	public static boolean isValid(String name) {
		try {
			valueOf(name);
		} catch (RuntimeException e) {
			return false;
		}

		return true;
	}
}