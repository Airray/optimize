package edu.duke.starfish.profile.profileinfo.execution;

public enum MRExecutionStatus {
	SUCCESS, FAILED, KILLED, PREP, RUNNING;

	public String getDescription()
  {
    switch (1.$SwitchMap$edu$duke$starfish$profile$profileinfo$execution$MRExecutionStatus[ordinal()]) {
    case 1:
      return "Success";
    case 2:
      return "Failed";
    case 3:
      return "Killed";
    case 4:
      return "In Prep";
    case 5:
      return "Running";
    }
    return toString();
  }
}