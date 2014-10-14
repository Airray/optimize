package edu.duke.starfish.profile.junit;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.utils.XMLProfileParser;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import junit.framework.TestCase;
import org.junit.Test;

public class TestXMLProfileParser extends TestCase
{
  @Test
  public void testImportExportProfile()
  {
    MRJobProfile profile = JUnitUtils.getTeraSortJobProfile();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    XMLProfileParser parser = new XMLProfileParser();
    parser.exportXML(profile, ps);

    String content = null;
    try {
      content = baos.toString("UTF-8");
      MRJobProfile newProfile = (MRJobProfile)parser.importXML(new ByteArrayInputStream(content.getBytes("UTF-8")));

      assertEquals(profile.getJobId(), newProfile.getJobId());
      assertEquals(profile.getAvgMapProfiles(), newProfile.getAvgMapProfiles());

      assertEquals(profile.getAvgReduceProfile(), newProfile.getAvgReduceProfile());

      assertEquals(profile.getCostFactors(), newProfile.getCostFactors());
      assertEquals(profile.getCounters(), newProfile.getCounters());
      assertEquals(profile.getStatistics(), newProfile.getStatistics());
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testImportExportProfileMapOnly() {
    MRJobProfile profile = JUnitUtils.getTeraSortJobProfile();

    ((MRMapProfile)profile.getAvgMapProfiles().get(0)).addCounter(MRCounter.MAP_MAX_UNIQUE_GROUPS, Long.valueOf(0L));

    ((MRMapProfile)profile.getMapProfiles().get(0)).addCounter(MRCounter.MAP_MAX_UNIQUE_GROUPS, Long.valueOf(0L));

    profile.getReduceProfiles().clear();
    profile.getAvgReduceProfile().clearProfile();
    profile.updateProfile();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    XMLProfileParser parser = new XMLProfileParser();
    parser.exportXML(profile, ps);

    String content = null;
    try {
      content = baos.toString("UTF-8");
      MRJobProfile newProfile = (MRJobProfile)parser.importXML(new ByteArrayInputStream(content.getBytes("UTF-8")));

      assertEquals(profile.getJobId(), newProfile.getJobId());
      assertEquals(profile.getAvgMapProfiles(), newProfile.getAvgMapProfiles());

      assertEquals(profile.getAvgReduceProfile(), newProfile.getAvgReduceProfile());

      assertEquals(profile.getCostFactors(), newProfile.getCostFactors());
      assertEquals(profile.getCounters(), newProfile.getCounters());
      assertEquals(profile.getStatistics(), newProfile.getStatistics());
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }
}