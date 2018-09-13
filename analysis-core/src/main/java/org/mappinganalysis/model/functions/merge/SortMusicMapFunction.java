package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.MergeTuple;

/**
 */
public class SortMusicMapFunction
    implements MapFunction<MergeTriplet, MergeTriplet> {
  private static final Logger LOG = Logger.getLogger(SortMusicMapFunction.class);

  @Override
  public MergeTriplet map(MergeTriplet triplet) throws Exception {
    if (triplet.getSrcId() > triplet.getTrgId()) {
      MergeTuple tmpTuple = triplet.getSrcTuple();

      triplet.setSrcId(triplet.getTrgId());
      triplet.setSrcTuple(triplet.getTrgTuple());

      triplet.setTrgId(tmpTuple.getId());
      triplet.setTrgTuple(tmpTuple);
    }
    return triplet;
  }
}
