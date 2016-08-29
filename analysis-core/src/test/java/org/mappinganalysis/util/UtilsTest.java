package org.mappinganalysis.util;

import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class UtilsTest {
  private static final Logger LOG = Logger.getLogger(UtilsTest.class);

  @Test
  public void testGetShadingTypes() throws Exception {
    Set<String> first = Sets.newHashSet("School", "ArchitecturalStructure");
    assertEquals(1, Utils.getShadingTypes(first).size());

    Set<String> second = Sets.newHashSet("Country", "AdministrativeRegion", "Settlement");
    assertEquals(1, Utils.getShadingTypes(second).size());

    Set<String> third = Sets.newHashSet("Mountain", "Island");
    assertEquals(1, Utils.getShadingTypes(third).size());

    first.addAll(second);
    first.addAll(third);
    first.add("Park");
    assertEquals(4, Utils.getShadingTypes(first).size());
  }

  @Test
  public void testGetHash() throws Exception {

  }
}