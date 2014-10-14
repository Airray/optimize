package edu.duke.starfish.profile.profileinfo.execution;

public enum DataLocality {
	DATA_LOCAL, RACK_LOCAL, NON_LOCAL;

	public String getDescription()
  {
    switch (1.$SwitchMap$edu$duke$starfish$profile$profileinfo$execution$DataLocality[ordinal()]) {
    case 1:
      return "Data Local";
    case 2:
      return "Rack Local";
    case 3:
      return "Non Local";
    }
    return toString();
  }
}