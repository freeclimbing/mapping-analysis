package org.mappinganalysis.model.functions.preprocessing;

import org.junit.Test;
import org.mappinganalysis.model.functions.preprocessing.utils.ComponentSourceTuple;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;

import java.util.HashMap;

import static org.junit.Assert.*;

public class ComponentSourceTupleTest {
  private static final HashMap<String, Integer> SOURCES = Constants.GEO_MAP;

  @Test
  public void testAddSource() throws Exception {
    ComponentSourceTuple tuple = new ComponentSourceTuple(1L, SOURCES);
    tuple.addSource(Constants.DBP_NS);
    tuple.addSource(Constants.GN_NS);
    tuple.addSource(Constants.LGD_NS);
    tuple.addSource(Constants.FB_NS);
    tuple.addSource(Constants.NYT_NS);

    assertEquals(31, tuple.getSourcesInt().intValue());
    assertEquals(5, AbstractionUtils.getSourceCount(tuple).intValue());

    ComponentSourceTuple differentOrderTuple = new ComponentSourceTuple(2L, SOURCES);
    differentOrderTuple.addSource(Constants.NYT_NS);
    differentOrderTuple.addSource(Constants.FB_NS);
    differentOrderTuple.addSource(Constants.LGD_NS);
    differentOrderTuple.addSource(Constants.GN_NS);
    differentOrderTuple.addSource(Constants.DBP_NS);

    assertEquals(31, differentOrderTuple.getSourcesInt().intValue());

    ComponentSourceTuple maxSingleValue = new ComponentSourceTuple(3L, SOURCES);
    maxSingleValue.addSource(Constants.GN_NS);
    maxSingleValue.addSource(Constants.GN_NS);

    assertEquals(16, maxSingleValue.getSourcesInt().intValue());


    ComponentSourceTuple minSingleValue = new ComponentSourceTuple(4L, SOURCES);
    minSingleValue.addSource(Constants.NYT_NS);
    minSingleValue.addSource(Constants.NYT_NS);

    assertEquals(1, minSingleValue.getSourcesInt().intValue());

    ComponentSourceTuple notOverfloatingTuple = new ComponentSourceTuple(5L, SOURCES);
    notOverfloatingTuple.addSource(Constants.DBP_NS);
    notOverfloatingTuple.addSource(Constants.NYT_NS);
    notOverfloatingTuple.addSource(Constants.GN_NS);
    notOverfloatingTuple.addSource(Constants.FB_NS);
    notOverfloatingTuple.addSource(Constants.LGD_NS);
    notOverfloatingTuple.addSource(Constants.DBP_NS);
    notOverfloatingTuple.addSource(Constants.NYT_NS);
    notOverfloatingTuple.addSource(Constants.GN_NS);
    notOverfloatingTuple.addSource(Constants.FB_NS);
    notOverfloatingTuple.addSource(Constants.LGD_NS);
    notOverfloatingTuple.addSource(Constants.DBP_NS);
    notOverfloatingTuple.addSource(Constants.NYT_NS);
    notOverfloatingTuple.addSource(Constants.GN_NS);
    notOverfloatingTuple.addSource(Constants.FB_NS);
    notOverfloatingTuple.addSource(Constants.LGD_NS);

    assertEquals(31, notOverfloatingTuple.getSourcesInt().intValue());
  }

  @Test
  public void testContainsSrc() throws Exception {
    ComponentSourceTuple tuple = new ComponentSourceTuple(1L, SOURCES);
    tuple.addSource(Constants.DBP_NS);
    tuple.addSource(Constants.GN_NS);
    tuple.addSource(Constants.LGD_NS);
    assertTrue(tuple.contains(Constants.DBP_NS));
    assertTrue(tuple.contains(Constants.GN_NS));
    assertFalse(tuple.contains(Constants.FB_NS));

    ComponentSourceTuple tuple2 = new ComponentSourceTuple(2L, SOURCES);
    tuple2.addSource(Constants.NYT_NS);
    tuple2.addSource(Constants.FB_NS);
    tuple2.addSource(Constants.LGD_NS);
    tuple2.addSource(Constants.GN_NS);
    tuple2.addSource(Constants.DBP_NS);
    assertTrue(tuple2.contains(Constants.DBP_NS)
        && tuple2.contains(Constants.GN_NS)
        && tuple2.contains(Constants.FB_NS)
        && tuple2.contains(Constants.NYT_NS)
        && tuple2.contains(Constants.LGD_NS));

    ComponentSourceTuple tuple3 = new ComponentSourceTuple(3L, SOURCES);
    assertFalse(tuple3.contains(Constants.DBP_NS)
        && tuple3.contains(Constants.GN_NS)
        && tuple3.contains(Constants.FB_NS)
        && tuple3.contains(Constants.NYT_NS)
        && tuple3.contains(Constants.LGD_NS));

  }

  @Test
  public void testGetSources() throws Exception {

  }

  @Test
  public void testGetSourceCount() throws Exception {

  }
}