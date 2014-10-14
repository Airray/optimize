package edu.duke.starfish.profile.profileinfo.execution.profile;

import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public abstract class MRExecProfile
{
  private Map<MRCounter, Long> counters;
  private Map<MRStatistics, Double> stats;
  private Map<MRCostFactors, Double> costs;
  private Map<String, Long> auxCounters;

  public MRExecProfile()
  {
    this.counters = null;
    this.stats = null;
    this.costs = null;
    this.auxCounters = null;
  }

  public MRExecProfile(MRExecProfile other)
  {
    this();
    if (other.counters != null)
      this.counters = new EnumMap(other.counters);
    if (other.stats != null)
      this.stats = new EnumMap(other.stats);
    if (other.costs != null)
      this.costs = new EnumMap(other.costs);
    if (other.auxCounters != null)
      this.auxCounters = new HashMap(other.auxCounters);
  }

  public Map<MRCounter, Long> getCounters()
  {
    if (this.counters == null)
      this.counters = new EnumMap(MRCounter.class);
    return this.counters;
  }

  public Map<MRStatistics, Double> getStatistics()
  {
    if (this.stats == null)
      this.stats = new EnumMap(MRStatistics.class);
    return this.stats;
  }

  public Map<MRCostFactors, Double> getCostFactors()
  {
    if (this.costs == null)
      this.costs = new EnumMap(MRCostFactors.class);
    return this.costs;
  }

  public Map<String, Long> getAuxCounters()
  {
    if (this.auxCounters == null)
      this.auxCounters = new HashMap();
    return this.auxCounters;
  }

  public void setCounters(Map<MRCounter, Long> counters)
  {
    this.counters = counters;
  }

  public void setStatistics(Map<MRStatistics, Double> stats)
  {
    this.stats = stats;
  }

  public void setCostFactors(Map<MRCostFactors, Double> costs)
  {
    this.costs = costs;
  }

  public void setAuxCounters(Map<String, Long> auxCounters)
  {
    this.auxCounters = auxCounters;
  }

  public void addCounter(MRCounter counter, Long value)
  {
    if (this.counters == null)
      this.counters = new EnumMap(MRCounter.class);
    this.counters.put(counter, value);
  }

  public void addStatistic(MRStatistics stat, Double value)
  {
    if (this.stats == null)
      this.stats = new EnumMap(MRStatistics.class);
    this.stats.put(stat, value);
  }

  public void addCostFactor(MRCostFactors cost, Double value)
  {
    if (this.costs == null)
      this.costs = new EnumMap(MRCostFactors.class);
    this.costs.put(cost, value);
  }

  public void addAuxCounter(String auxCounter, Long value)
  {
    if (this.auxCounters == null)
      this.auxCounters = new HashMap();
    this.auxCounters.put(auxCounter, value);
  }

  public void addCounters(Map<MRCounter, Long> counters)
  {
    if (this.counters == null)
      this.counters = new EnumMap(MRCounter.class);
    this.counters.putAll(counters);
  }

  public void addStatistics(Map<MRStatistics, Double> stats)
  {
    if (this.stats == null)
      this.stats = new EnumMap(MRStatistics.class);
    this.stats.putAll(stats);
  }

  public void addCostFactors(Map<MRCostFactors, Double> costs)
  {
    if (this.costs == null)
      this.costs = new EnumMap(MRCostFactors.class);
    this.costs.putAll(costs);
  }

  public void addAuxCounters(Map<String, Long> auxCounters)
  {
    if (this.auxCounters == null)
      this.auxCounters = new HashMap();
    this.auxCounters.putAll(auxCounters);
  }

  public boolean containsCounter(MRCounter counter)
  {
    return (this.counters != null) && (this.counters.containsKey(counter));
  }

  public boolean containsStatistic(MRStatistics stat)
  {
    return (this.stats != null) && (this.stats.containsKey(stat));
  }

  public boolean containsCostFactor(MRCostFactors cost)
  {
    return (this.costs != null) && (this.costs.containsKey(cost));
  }

  public boolean containsAuxCounter(String auxCounter)
  {
    return (this.auxCounters != null) && (this.auxCounters.containsKey(auxCounter));
  }

  public boolean containsAuxCounters()
  {
    return (this.auxCounters != null) && (!this.auxCounters.isEmpty());
  }

  public Long getCounter(MRCounter counter)
  {
    return this.counters == null ? null : (Long)this.counters.get(counter);
  }

  public Long getCounter(MRCounter counter, Long defaultValue)
  {
    if ((this.counters == null) || (!this.counters.containsKey(counter))) {
      return defaultValue;
    }
    return (Long)this.counters.get(counter);
  }

  public Double getStatistic(MRStatistics stat)
  {
    return this.stats == null ? null : (Double)this.stats.get(stat);
  }

  public Double getStatistic(MRStatistics stat, Double defaultValue)
  {
    if ((this.stats == null) || (!this.stats.containsKey(stat))) {
      return defaultValue;
    }
    return (Double)this.stats.get(stat);
  }

  public Double getCostFactor(MRCostFactors cost)
  {
    return this.costs == null ? null : (Double)this.costs.get(cost);
  }

  public Double getCostFactor(MRCostFactors cost, Double defaultValue)
  {
    if ((this.costs == null) || (!this.costs.containsKey(cost))) {
      return defaultValue;
    }
    return (Double)this.costs.get(cost);
  }

  public Long getAuxCounter(String auxCounter)
  {
    return this.auxCounters == null ? null : (Long)this.auxCounters.get(auxCounter);
  }

  public Long getAuxCounter(String auxCounter, Long defaultValue)
  {
    if ((this.auxCounters == null) || (!this.auxCounters.containsKey(auxCounter))) {
      return defaultValue;
    }
    return (Long)this.auxCounters.get(auxCounter);
  }

  public boolean isEmpty()
  {
    return ((this.counters == null) || (this.counters.isEmpty())) && ((this.stats == null) || (this.stats.isEmpty())) && ((this.costs == null) || (this.costs.isEmpty())) && ((this.auxCounters == null) || (this.auxCounters.isEmpty()));
  }

  public void clearProfile()
  {
    if (this.counters != null)
      this.counters.clear();
    if (this.stats != null)
      this.stats.clear();
    if (this.costs != null)
      this.costs.clear();
    if (this.auxCounters != null)
      this.auxCounters.clear();
  }

  public int hashCode()
  {
    int hash = 1;
    hash = 31 * hash + (this.costs == null ? 0 : this.costs.hashCode());
    hash = 37 * hash + (this.counters == null ? 0 : this.counters.hashCode());
    hash = 41 * hash + (this.stats == null ? 0 : this.stats.hashCode());
    hash = 43 * hash + (this.auxCounters == null ? 0 : this.auxCounters.hashCode());
    return hash;
  }

  public boolean equals(Object obj)
  {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof MRExecProfile))
      return false;
    MRExecProfile other = (MRExecProfile)obj;
    if (this.costs == null) {
      if (other.costs != null)
        return false;
    } else if (!this.costs.equals(other.costs))
      return false;
    if (this.counters == null) {
      if (other.counters != null)
        return false;
    } else if (!this.counters.equals(other.counters))
      return false;
    if (this.stats == null) {
      if (other.stats != null)
        return false;
    } else if (!this.stats.equals(other.stats))
      return false;
    if (this.auxCounters == null) {
      if (other.auxCounters != null)
        return false;
    } else if (!this.auxCounters.equals(other.auxCounters))
      return false;
    return true;
  }

  public String toString()
  {
    return new StringBuilder().append("MRExecProfile [counters=").append(this.counters == null ? 0 : this.counters.size()).append(", stats=").append(this.stats == null ? 0 : this.stats.size()).append(", costs=").append(this.costs == null ? 0 : this.costs.size()).append(", auxCounters=").append(this.auxCounters == null ? 0 : this.auxCounters.size()).append("]").toString();
  }

  protected void printEnumToNumberMap(PrintStream out, Map<? extends Enum<?>, ?> map, NumberFormat nf)
  {
    for (Map.Entry entry : map.entrySet())
      out.println(new StringBuilder().append("\t").append(entry.getKey()).append("\t").append(nf.format(entry.getValue())).toString());
  }

  protected void printStringToNumberMap(PrintStream out, Map<String, ?> map, NumberFormat nf)
  {
    for (Map.Entry entry : map.entrySet())
      out.println(new StringBuilder().append("\t").append((String)entry.getKey()).append("\t").append(nf.format(entry.getValue())).toString());
  }
}