package org.mappinganalysis.model.functions.preprocessing;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.SourcesUtils;

import static org.junit.Assert.*;

/** todo use assert!
 */
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

    LOG.info(tuple.toString() + " sourceCount: " + SourcesUtils.getSourceCount(tuple)
    + " sources: " + tuple.getSources());
    LOG.info(tuple2.toString());
    LOG.info(tuple3.toString());
    LOG.info(tuple4.toString());
    LOG.info(tuple5.toString());
  }

  @Test
  public void testContainsSrc() throws Exception {
    ComponentSourceTuple tuple = new ComponentSourceTuple(1L);
    tuple.addSource(Constants.DBP_NS);
    tuple.addSource(Constants.GN_NS);
    tuple.addSource(Constants.LGD_NS);

    LOG.info("dbp: " + tuple.contains(Constants.DBP_NS));
    LOG.info("gn: " + tuple.contains(Constants.GN_NS));
    LOG.info("fb: " + tuple.contains(Constants.FB_NS));

    ComponentSourceTuple tuple2 = new ComponentSourceTuple(2L);
    tuple2.addSource(Constants.NYT_NS);
    tuple2.addSource(Constants.FB_NS);
    tuple2.addSource(Constants.LGD_NS);
    tuple2.addSource(Constants.GN_NS);
    tuple2.addSource(Constants.DBP_NS);

    LOG.info("dbp: " + tuple2.contains(Constants.DBP_NS));
    LOG.info("gn: " + tuple2.contains(Constants.GN_NS));
    LOG.info("fb: " + tuple2.contains(Constants.FB_NS));
    LOG.info("nyt: " + tuple2.contains(Constants.NYT_NS));
    LOG.info("lgd: " + tuple2.contains(Constants.LGD_NS));

    ComponentSourceTuple tuple3 = new ComponentSourceTuple(3L);
    LOG.info("dbp: " + tuple3.contains(Constants.DBP_NS));
    LOG.info("gn: " + tuple3.contains(Constants.GN_NS));
    LOG.info("fb: " + tuple3.contains(Constants.FB_NS));
    LOG.info("nyt: " + tuple3.contains(Constants.NYT_NS));
    LOG.info("lgd: " + tuple3.contains(Constants.LGD_NS));

  }

  @Test
  public void testGetSources() throws Exception {

  }

  @Test
  public void testGetSourceCount() throws Exception {

  }
}