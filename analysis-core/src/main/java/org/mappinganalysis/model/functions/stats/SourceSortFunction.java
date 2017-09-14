package org.mappinganalysis.model.functions.stats;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.log4j.Logger;
import org.mappinganalysis.util.Constants;

/**
 * Sort certain data source values infront of others, slightly unsmart because five hard coded
 * data sources.
 */
public class SourceSortFunction implements MapFunction<Tuple4<Long, Long, String, String>,
    Tuple3<String, String, Integer>> {
  private static final Logger LOG = Logger.getLogger(SourceSortFunction.class);

  private Tuple3<String, String, Integer> reuseTuple;

  public SourceSortFunction() {
    this.reuseTuple = new Tuple3<>();
  }

  @Override
  public Tuple3<String, String, Integer> map(Tuple4<Long, Long, String, String> tuple) throws Exception {
    if (tuple.f2.equals(Constants.DBP_NS)) {
      reuseTuple.setFields(tuple.f2, tuple.f3, 1);
      return reuseTuple;
    } else if (tuple.f3.equals(Constants.DBP_NS)) {
      reuseTuple.setFields(tuple.f3, tuple.f2, 1);
      return reuseTuple;
    } else if (tuple.f2.equals(Constants.GN_NS)) {
      reuseTuple.setFields(tuple.f2, tuple.f3, 1);
      return reuseTuple;
    } else if (tuple.f3.equals(Constants.GN_NS)) {
      reuseTuple.setFields(tuple.f3, tuple.f2, 1);
      return reuseTuple;
    } else if (tuple.f2.equals(Constants.LGD_NS)) {
      reuseTuple.setFields(tuple.f2, tuple.f3, 1);
      return reuseTuple;
    } else if (tuple.f3.equals(Constants.LGD_NS)) {
      reuseTuple.setFields(tuple.f3, tuple.f2, 1);
      return reuseTuple;
    } else if (tuple.f2.equals(Constants.FB_NS)) {
      reuseTuple.setFields(tuple.f2, tuple.f3, 1);
      return reuseTuple;
    } else if (tuple.f3.equals(Constants.FB_NS)) {
      reuseTuple.setFields(tuple.f3, tuple.f2, 1);
      return reuseTuple;
    } else {
      LOG.info("### should not happen");
      return reuseTuple;
    }
  }
}
