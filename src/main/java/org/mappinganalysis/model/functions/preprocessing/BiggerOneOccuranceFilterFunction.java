package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.log4j.Logger;

/**
 * Filter tuples with size bigger than 1.
 */
public class BiggerOneOccuranceFilterFunction implements FilterFunction<Tuple7<Long, Long, Long, String, Integer, Double, Long>> {
  private static final Logger LOG = Logger.getLogger(BiggerOneOccuranceFilterFunction.class);

  @Override
  public boolean filter(Tuple7<Long, Long, Long, String, Integer, Double, Long> tuple)
      throws Exception {
    if (tuple.f4 > 1) {
      LOG.info("biggerOneTuple: " + tuple);
    }
    return tuple.f4 > 1;
  }
}
