package edu.duke.starfish.profile.junit;

import edu.duke.starfish.profile.utils.GeneralUtils;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.junit.Test;

public class TestUtilityMethods extends TestCase
{
  @Test
  public void testNormalizePath()
  {
    assertEquals("/some/dir", GeneralUtils.normalizePath("hdfs://host:9000/some/dir"));

    assertEquals("/some/dir", GeneralUtils.normalizePath("s3n://bucket/some/dir"));

    assertEquals("/some/dir", GeneralUtils.normalizePath("file:///some/dir"));

    assertEquals("/some/dir", GeneralUtils.normalizePath("/some/dir"));
    assertEquals("/some/dir", GeneralUtils.normalizePath("/some/dir/"));
    assertEquals("/", GeneralUtils.normalizePath("hdfs://localhost:9000/"));
  }

  @Test
  public void testUnionPaths() {
    List union = new ArrayList();

    GeneralUtils.unionPathsWithOrder(union, new String[] { "/path1/file1", "/path1/file2", "hdfs://localhost/path2" });

    assertEquals("[/path1/file1, /path1/file2, /path2]", union.toString());

    GeneralUtils.unionPathsWithOrder(union, new String[] { "/path1/file1", "/path2" });
    assertEquals("[/path1/file1, /path1/file2, /path2]", union.toString());

    GeneralUtils.unionPathsWithOrder(union, new String[] { "/path2/file1", "/path2/file1" });
    assertEquals("[/path1/file1, /path1/file2, /path2, /path2/file1]", union.toString());
  }
}