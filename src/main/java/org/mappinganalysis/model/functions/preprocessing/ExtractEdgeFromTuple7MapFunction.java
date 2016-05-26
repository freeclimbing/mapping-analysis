package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.log4j.Logger;

/**
 * Created by markus on 5/24/16.
 */
public class ExtractEdgeFromTuple7MapFunction implements MapFunction<Tuple7<Long, Long, Long, String, Integer, Double, Long>,
    Tuple2<Long, Long>> {
  private static final Logger LOG = Logger.getLogger(ExtractEdgeFromTuple7MapFunction.class);

  @Override
  public Tuple2<Long, Long> map(Tuple7<Long, Long, Long, String, Integer, Double, Long> value)
      throws Exception {
    LOG.info("tmpTuple7: " + value);
    return new Tuple2<>(value.f0, value.f1);
  }
}
