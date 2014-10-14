package edu.duke.starfish.profile.profileinfo.execution.profile.enums;

public enum MRTaskPhase {
	SHUFFLE, SORT, SETUP, READ, MAP, REDUCE, COLLECT, WRITE, SPILL, MERGE, CLEANUP;

	public String getName()
  {
    switch (1.$SwitchMap$edu$duke$starfish$profile$profileinfo$execution$profile$enums$MRTaskPhase[ordinal()]) {
    case 1:
      return "SHUFFLE";
    case 2:
      return "MERGE";
    case 3:
      return "SETUP";
    case 4:
      return "READ";
    case 5:
      return "MAP";
    case 6:
      return "REDUCE";
    case 7:
      return "COLLECT";
    case 8:
      return "WRITE";
    case 9:
      return "CLEANUP";
    case 10:
      return "SPILL";
    case 11:
      return "MERGE";
    }
    return toString();
  }

	public String getDescription()
  {
    switch (1.$SwitchMap$edu$duke$starfish$profile$profileinfo$execution$profile$enums$MRTaskPhase[ordinal()]) {
    case 1:
      return "Shuffle phase: Transferring map output data to reduce tasks, with decompression if needed";
    case 2:
      return "Merge phase: Merging sorted map outputs";
    case 3:
      return "Setup phase: Executing the user-defined setup function";
    case 4:
      return "Read phase: Reading the job input data from the distributed filesystem";
    case 5:
      return "Map phase: Executing the user-defined map function";
    case 6:
      return "Reduce phase: Executing the user-defined reduce function";
    case 7:
      return "Collect phase: Partitioning and serializing the map output data to buffer before spilling";
    case 8:
      return "Write phase: Writing the job output data to the distributed filesystem";
    case 9:
      return "Cleanup phase: Executing the user-defined task cleanup function";
    case 10:
      return "Spill phase: Sorting, combining, compressing, and writing map output data to local disk";
    case 11:
      return "Merge phase: Merging sorted spill files";
    }
    return toString();
  }
}