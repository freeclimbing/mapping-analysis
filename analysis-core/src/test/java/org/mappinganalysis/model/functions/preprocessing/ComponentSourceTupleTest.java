package org.mappinganalysis.model.functions.preprocessing;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.util.Constants;

import static org.junit.Assert.*;

public class ComponentSourceTupleTest {
  private static final Logger LOG = Logger.getLogger(ComponentSourceTupleTest.class);

  @Test
  public void testAddSource() throws Exception {
    ComponentSourceTuple tuple = new ComponentSourceTuple(1L);
    tuple.addSource(Constants.DBP_NS);
    tuple.addSource(Constants.GN_NS);
    tuple.addSource(Constants.LGD_NS);
    tuple.addSource(Constants.FB_NS);
    tuple.addSource(Constants.NYT_NS);

    ComponentSourceTuple tuple2 = new ComponentSourceTuple(2L);
    tuple2.addSource(Constants.NYT_NS);
    tuple2.addSource(Constants.FB_NS);
    tuple2.addSource(Constants.LGD_NS);
    tuple2.addSource(Constants.GN_NS);
    tuple2.addSource(Constants.DBP_NS);

    ComponentSourceTuple tuple3 = new ComponentSourceTuple(3L);
    tuple3.addSource(Constants.NYT_NS);
    tuple3.addSource(Constants.NYT_NS);

    ComponentSourceTuple tuple4 = new ComponentSourceTuple(4L);
    tuple4.addSource(Constants.DBP_NS);
    tuple4.addSource(Constants.DBP_NS);

    ComponentSourceTuple tuple5 = new ComponentSourceTuple(5L);
    tuple5.addSource(Constants.DBP_NS);
    tuple5.addSource(Constants.NYT_NS);
    tuple5.addSource(Constants.GN_NS);
    tuple5.addSource(Constants.FB_NS);
    tuple5.addSource(Constants.LGD_NS);
    tuple5.addSource(Constants.DBP_NS);
    tuple5.addSource(Constants.NYT_NS);
    tuple5.addSource(Constants.GN_NS);
    tuple5.addSource(Constants.FB_NS);
    tuple5.addSource(Constants.LGD_NS);
    tuple5.addSource(Constants.DBP_NS);
    tuple5.addSource(Constants.NYT_NS);
    tuple5.addSource(Constants.GN_NS);
    tuple5.addSource(Constants.FB_NS);
    tuple5.addSource(Constants.LGD_NS);

    LOG.info(tuple.toString() + " sourceCount: " + tuple.getSourceCount()
    + " sources: " + tuple.getSources());
    LOG.info(tuple2.toString());
    LOG.info(tuple3.toString());
    LOG.info(tuple4.toString());
    LOG.info(tuple5.toString());
  }

  @Test
  public void testGetCcId() throws Exception {

  }

  @Test
  public void testGetSources() throws Exception {

  }

  @Test
  public void testGetSourceCount() throws Exception {

  }
}