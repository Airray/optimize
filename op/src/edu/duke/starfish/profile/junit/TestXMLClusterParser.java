package edu.duke.starfish.profile.junit;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.utils.XMLClusterParser;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import junit.framework.TestCase;
import org.junit.Test;

public class TestXMLClusterParser extends TestCase
{
  @Test
  public void testImportExportCluster()
  {
    ClusterConfiguration cluster = JUnitUtils.getClusterConfiguration();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    XMLClusterParser parser = new XMLClusterParser();
    parser.exportXML(cluster, ps);

    String content = null;
    try {
      content = baos.toString("UTF-8");
      ClusterConfiguration newCluster = (ClusterConfiguration)parser.importXML(new ByteArrayInputStream(content.getBytes("UTF-8")));

      assertEquals(cluster, newCluster);
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testImportClusterFromSpecs() {
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><cluster name=\"local\"><specs num_racks=\"2\" hosts_per_rack=\"3\" map_slots_per_host=\"3\" reduce_slots_per_host=\"1\" max_slot_memory=\"100\" /></cluster>";
    try
    {
      ClusterConfiguration expCluster = ClusterConfiguration.createClusterConfiguration("local", 2, 3, 3, 1, 104857600L, 104857600L);

      XMLClusterParser parser = new XMLClusterParser();
      ClusterConfiguration newCluster = (ClusterConfiguration)parser.importXML(new ByteArrayInputStream(xml.getBytes("UTF-8")));

      assertEquals(expCluster, newCluster);
    }
    catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      fail();
    }
  }
}