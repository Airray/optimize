package edu.duke.starfish.profile.profileinfo.setup;

public class TaskTrackerInfo extends TrackerInfo
{
  private int numMapSlots;
  private int numReduceSlots;
  private long maxMapTaskMemory;
  private long maxRedTaskMemory;

  public TaskTrackerInfo()
  {
    this.numMapSlots = 2;
    this.numReduceSlots = 2;
    this.maxMapTaskMemory = 209715200L;
    this.maxRedTaskMemory = 209715200L;
  }

  public TaskTrackerInfo(long internalId, String name, String hostName, int port, int numMapSlots, int numReduceSlots, long maxMapSlotMemory, long maxRedSlotMemory)
  {
    super(internalId, name, hostName, port);
    this.numMapSlots = numMapSlots;
    this.numReduceSlots = numReduceSlots;
    this.maxMapTaskMemory = maxMapSlotMemory;
    this.maxRedTaskMemory = maxRedSlotMemory;
  }

  public TaskTrackerInfo(TaskTrackerInfo other)
  {
    super(other);
    this.numMapSlots = other.numMapSlots;
    this.numReduceSlots = other.numReduceSlots;
    this.maxMapTaskMemory = other.maxMapTaskMemory;
    this.maxRedTaskMemory = other.maxRedTaskMemory;
  }

  public int getNumMapSlots()
  {
    return this.numMapSlots;
  }

  public int getNumReduceSlots()
  {
    return this.numReduceSlots;
  }

  public long getMaxMapTaskMemory()
  {
    return this.maxMapTaskMemory;
  }

  public long getMaxReduceTaskMemory()
  {
    return this.maxRedTaskMemory;
  }

  public void setNumMapSlots(int numMapSlots)
  {
    this.hash = -1;
    this.numMapSlots = numMapSlots;
  }

  public void setNumReduceSlots(int numReduceSlots)
  {
    this.hash = -1;
    this.numReduceSlots = numReduceSlots;
  }

  public void setMaxMapTaskMemory(long maxSlotMemory)
  {
    this.hash = -1;
    this.maxMapTaskMemory = maxSlotMemory;
  }

  public void setMaxReduceTaskMemory(long maxSlotMemory)
  {
    this.hash = -1;
    this.maxRedTaskMemory = maxSlotMemory;
  }

  public int hashCode()
  {
    if (this.hash == -1) {
      this.hash = super.hashCode();
      this.hash = (31 * this.hash + this.numMapSlots);
      this.hash = (37 * this.hash + this.numReduceSlots);
      this.hash = (41 * this.hash + (int)(this.maxMapTaskMemory ^ this.maxMapTaskMemory >>> 32));

      this.hash = (43 * this.hash + (int)(this.maxRedTaskMemory ^ this.maxRedTaskMemory >>> 32));
    }

    return this.hash;
  }

  public boolean equals(Object obj)
  {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (!(obj instanceof TaskTrackerInfo))
      return false;
    TaskTrackerInfo other = (TaskTrackerInfo)obj;
    if (this.numMapSlots != other.numMapSlots)
      return false;
    if (this.numReduceSlots != other.numReduceSlots)
      return false;
    if (this.maxMapTaskMemory != other.maxMapTaskMemory) {
      return false;
    }
    return this.maxRedTaskMemory == other.maxRedTaskMemory;
  }

  public String toString()
  {
    return "TaskTrackerInfo [Name=" + getName() + ", Host=" + getHostName() + ", Port=" + getPort() + "]";
  }
}