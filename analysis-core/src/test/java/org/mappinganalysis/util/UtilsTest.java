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
  public void testTypeSim() throws Exception {
    Set<String> first = Sets.newHashSet("ArchitecturalStructure");
    Set<String> second = Sets.newHashSet("Country", "AdministrativeRegion");
    assertEquals(0d, Utils.getTypeSim(first, second), 0.01);

    second.add("School");
    assertEquals(1d, Utils.getTypeSim(first, second), 0.01);

    first = Sets.newHashSet("Settlement");
    second = Sets.newHashSet("Country");
    assertEquals(1d, Utils.getTypeSim(first, second), 0.01);

    // no type should not occur here, still tested
    second = Sets.newHashSet(Constants.NO_TYPE);
    assertEquals(0d, Utils.getTypeSim(first, second), 0.01);

    first = Sets.newHashSet(Constants.NO_TYPE);
    assertEquals(1d, Utils.getTypeSim(first, second), 0.01);
  }

//  @Test
//  public void testGetHash() throws Exception {
//
//  }
}