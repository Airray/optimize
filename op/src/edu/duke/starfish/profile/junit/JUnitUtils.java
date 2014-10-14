package edu.duke.starfish.profile.junit;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;

public class JUnitUtils
{
  public static ClusterConfiguration getClusterConfiguration()
  {
    ClusterConfiguration cluster = new ClusterConfiguration();

    TaskTrackerInfo taskTracker = null;
    for (int i = 0; i < 15; i++) {
      taskTracker = cluster.addFindTaskTrackerInfo("tracker-" + i, "/rack/host-" + i);

      taskTracker.setNumMapSlots(2);
      taskTracker.setNumReduceSlots(2);
    }

    cluster.addFindJobTrackerInfo("job_tracker", "/master-rack/master-host");

    return cluster;
  }

  public static MRJobProfile getTeraSortJobProfile()
  {
    MRJobProfile prof = new MRJobProfile("job_201011062135_0003");

    String[] inputs = { "hdfs://hadoop21.cs.duke.edu:9000/usr/research/home/hero/tera/in" };
    prof.setJobInputs(inputs);

    prof.addMapProfile(getTeraSortMapProfile());
    prof.addReduceProfile(getTeraSortReduceProfile());
    prof.updateProfile();

    prof.addCounter(MRCounter.MAP_TASKS, Long.valueOf(5L));
    prof.addCounter(MRCounter.REDUCE_TASKS, Long.valueOf(1L));

    return prof;
  }

  public static MRMapProfile getTeraSortMapProfile()
  {
    MRMapProfile prof = new MRMapProfile("aggegated_map_0_job_201011062135_0003");

    prof.addCounter(MRCounter.MAP_INPUT_RECORDS, Long.valueOf(200000L));
    prof.addCounter(MRCounter.MAP_INPUT_BYTES, Long.valueOf(20000000L));
    prof.addCounter(MRCounter.MAP_OUTPUT_RECORDS, Long.valueOf(200000L));
    prof.addCounter(MRCounter.MAP_OUTPUT_BYTES, Long.valueOf(20000000L));
    prof.addCounter(MRCounter.MAP_NUM_SPILLS, Long.valueOf(1L));
    prof.addCounter(MRCounter.MAP_NUM_SPILL_MERGES, Long.valueOf(0L));
    prof.addCounter(MRCounter.MAP_RECS_PER_BUFF_SPILL, Long.valueOf(200000L));
    prof.addCounter(MRCounter.MAP_SPILL_SIZE, Long.valueOf(2945155L));
    prof.addCounter(MRCounter.COMBINE_INPUT_RECORDS, Long.valueOf(0L));
    prof.addCounter(MRCounter.COMBINE_OUTPUT_RECORDS, Long.valueOf(0L));
    prof.addCounter(MRCounter.SPILLED_RECORDS, Long.valueOf(200000L));
    prof.addCounter(MRCounter.FILE_BYTES_READ, Long.valueOf(129L));
    prof.addCounter(MRCounter.FILE_BYTES_WRITTEN, Long.valueOf(2945187L));
    prof.addCounter(MRCounter.HDFS_BYTES_READ, Long.valueOf(20000000L));

    prof.addStatistic(MRStatistics.INPUT_PAIR_WIDTH, Double.valueOf(100.0D));
    prof.addStatistic(MRStatistics.MAP_SIZE_SEL, Double.valueOf(1.0D));
    prof.addStatistic(MRStatistics.MAP_PAIRS_SEL, Double.valueOf(1.0D));
    prof.addStatistic(MRStatistics.INTERM_COMPRESS_RATIO, Double.valueOf(0.14437D));
    prof.addStatistic(MRStatistics.STARTUP_MEM, Double.valueOf(5169355.2000000002D));
    prof.addStatistic(MRStatistics.SETUP_MEM, Double.valueOf(0.0D));
    prof.addStatistic(MRStatistics.MAP_MEM_PER_RECORD, Double.valueOf(33.297767999999998D));
    prof.addStatistic(MRStatistics.CLEANUP_MEM, Double.valueOf(0.0D));

    prof.addCostFactor(MRCostFactors.READ_HDFS_IO_COST, Double.valueOf(91.704462000000007D));
    prof.addCostFactor(MRCostFactors.READ_LOCAL_IO_COST, Double.valueOf(297.83816100000001D));
    prof.addCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST, Double.valueOf(1138.3313290000001D));
    prof.addCostFactor(MRCostFactors.MAP_CPU_COST, Double.valueOf(12568.075065000001D));
    prof.addCostFactor(MRCostFactors.PARTITION_CPU_COST, Double.valueOf(2580.2309890000001D));
    prof.addCostFactor(MRCostFactors.SERDE_CPU_COST, Double.valueOf(5420.8826099999997D));
    prof.addCostFactor(MRCostFactors.SORT_CPU_COST, Double.valueOf(329.77746300000001D));
    prof.addCostFactor(MRCostFactors.MERGE_CPU_COST, Double.valueOf(114.60798200000001D));
    prof.addCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST, Double.valueOf(396.04019499999998D));

    prof.addCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST, Double.valueOf(317.73301500000002D));
    prof.addCostFactor(MRCostFactors.SETUP_CPU_COST, Double.valueOf(1696252.3999999999D));
    prof.addCostFactor(MRCostFactors.CLEANUP_CPU_COST, Double.valueOf(232722.60000000001D));

    prof.addTiming(MRTaskPhase.SETUP, Double.valueOf(1.696252D));
    prof.addTiming(MRTaskPhase.READ, Double.valueOf(1834.0892389999999D));
    prof.addTiming(MRTaskPhase.MAP, Double.valueOf(2513.6150130000001D));
    prof.addTiming(MRTaskPhase.COLLECT, Double.valueOf(1600.22272D));
    prof.addTiming(MRTaskPhase.CLEANUP, Double.valueOf(0.232723D));
    prof.addTiming(MRTaskPhase.SPILL, Double.valueOf(10995.771881000001D));
    prof.addTiming(MRTaskPhase.MERGE, Double.valueOf(3.791427D));

    return prof;
  }

  public static MRReduceProfile getTeraSortReduceProfile()
  {
    MRReduceProfile prof = new MRReduceProfile("aggegated_reduce_job_201011062135_0003");

    prof.addCounter(MRCounter.REDUCE_SHUFFLE_BYTES, Long.valueOf(14725775L));
    prof.addCounter(MRCounter.REDUCE_INPUT_GROUPS, Long.valueOf(1000000L));
    prof.addCounter(MRCounter.REDUCE_INPUT_RECORDS, Long.valueOf(1000000L));
    prof.addCounter(MRCounter.REDUCE_INPUT_BYTES, Long.valueOf(102000010L));
    prof.addCounter(MRCounter.REDUCE_OUTPUT_RECORDS, Long.valueOf(1000000L));
    prof.addCounter(MRCounter.REDUCE_OUTPUT_BYTES, Long.valueOf(100000000L));
    prof.addCounter(MRCounter.COMBINE_INPUT_RECORDS, Long.valueOf(0L));
    prof.addCounter(MRCounter.COMBINE_OUTPUT_RECORDS, Long.valueOf(0L));
    prof.addCounter(MRCounter.SPILLED_RECORDS, Long.valueOf(1000000L));
    prof.addCounter(MRCounter.FILE_BYTES_READ, Long.valueOf(14885588L));
    prof.addCounter(MRCounter.FILE_BYTES_WRITTEN, Long.valueOf(14885588L));
    prof.addCounter(MRCounter.HDFS_BYTES_WRITTEN, Long.valueOf(15075873L));

    prof.addStatistic(MRStatistics.REDUCE_PAIRS_PER_GROUP, Double.valueOf(1.0D));
    prof.addStatistic(MRStatistics.REDUCE_SIZE_SEL, Double.valueOf(0.980392D));
    prof.addStatistic(MRStatistics.REDUCE_PAIRS_SEL, Double.valueOf(1.0D));
    prof.addStatistic(MRStatistics.INTERM_COMPRESS_RATIO, Double.valueOf(0.14437D));
    prof.addStatistic(MRStatistics.OUT_COMPRESS_RATIO, Double.valueOf(0.150759D));
    prof.addStatistic(MRStatistics.STARTUP_MEM, Double.valueOf(103362720.0D));
    prof.addStatistic(MRStatistics.SETUP_MEM, Double.valueOf(164848.0D));
    prof.addStatistic(MRStatistics.REDUCE_MEM_PER_RECORD, Double.valueOf(1.692152D));
    prof.addStatistic(MRStatistics.CLEANUP_MEM, Double.valueOf(0.0D));

    prof.addCostFactor(MRCostFactors.WRITE_HDFS_IO_COST, Double.valueOf(863.66438300000004D));
    prof.addCostFactor(MRCostFactors.READ_LOCAL_IO_COST, Double.valueOf(297.83816100000001D));
    prof.addCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST, Double.valueOf(1138.3313290000001D));
    prof.addCostFactor(MRCostFactors.NETWORK_COST, Double.valueOf(443.46122400000002D));
    prof.addCostFactor(MRCostFactors.REDUCE_CPU_COST, Double.valueOf(13630.213317D));
    prof.addCostFactor(MRCostFactors.MERGE_CPU_COST, Double.valueOf(114.60798200000001D));
    prof.addCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST, Double.valueOf(396.04019499999998D));

    prof.addCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST, Double.valueOf(323.93567300000001D));
    prof.addCostFactor(MRCostFactors.OUTPUT_COMPRESS_CPU_COST, Double.valueOf(276.21886499999999D));
    prof.addCostFactor(MRCostFactors.SETUP_CPU_COST, Double.valueOf(1521152.0D));
    prof.addCostFactor(MRCostFactors.CLEANUP_CPU_COST, Double.valueOf(79730.0D));

    prof.addTiming(MRTaskPhase.SHUFFLE, Double.valueOf(12362.165792D));
    prof.addTiming(MRTaskPhase.SORT, Double.valueOf(50120.369876999997D));
    prof.addTiming(MRTaskPhase.SETUP, Double.valueOf(1.521152D));
    prof.addTiming(MRTaskPhase.REDUCE, Double.valueOf(18016.111064000001D));
    prof.addTiming(MRTaskPhase.WRITE, Double.valueOf(40642.381100999999D));
    prof.addTiming(MRTaskPhase.CLEANUP, Double.valueOf(0.07973D));

    return prof;
  }
}