package edu.duke.starfish.profile.profileinfo.execution.mrtasks;

import edu.duke.starfish.profile.profileinfo.execution.ClusterExecutionInfo;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;
import java.util.Date;
import java.util.List;

public abstract class MRTaskInfo extends ClusterExecutionInfo
{
  public MRTaskInfo()
  {
  }

  public MRTaskInfo(long internalId, String execId, Date startTime, Date endTime, MRExecutionStatus status, String errorMsg)
  {
    super(internalId, execId, startTime, endTime, status, errorMsg);
  }

  public MRTaskInfo(MRTaskInfo other)
  {
    super(other);
  }

  public abstract MRTaskAttemptInfo getSuccessfulAttempt();

  public abstract List<? extends MRTaskAttemptInfo> getAttempts();
}