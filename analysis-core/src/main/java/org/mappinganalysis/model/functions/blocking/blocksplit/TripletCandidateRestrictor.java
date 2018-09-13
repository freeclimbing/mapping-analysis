package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;

/**
 * Within max both triplet creation, create the final music triplets.
 */
class TripletCandidateRestrictor
    implements FlatJoinFunction<
    Tuple2<MergeTuple,Long>,
    MergeTuple,
    MergeTriplet> {

  private DataDomain dataDomain;
  private String newSource;

  public TripletCandidateRestrictor(DataDomain dataDomain, String newSource) {
    this.dataDomain = dataDomain;
    this.newSource = newSource;
  }

  @Override
  public void join(
      Tuple2<MergeTuple, Long> first,
      MergeTuple second,
      Collector<MergeTriplet> out) throws Exception {
    MergeTriplet triplet = new MergeTriplet(first.f0, second);

    if (!newSource.equals(Constants.EMPTY_STRING)) {
      boolean sourceContains = AbstractionUtils
          .containsSrc(dataDomain, triplet.getSrcTuple().getIntSources(), newSource);
      int sourceCount = AbstractionUtils.getSourceCount(triplet.getSrcTuple().getIntSources());
      boolean targetContains = AbstractionUtils
          .containsSrc(dataDomain, triplet.getTrgTuple().getIntSources(), newSource);
      int targetCount = AbstractionUtils.getSourceCount(triplet.getTrgTuple().getIntSources());
      if (sourceContains && sourceCount == 1 || targetContains && targetCount == 1) {
        createResult(out, triplet);
      }
    } else {
      createResult(out, triplet);
    }
  }

  private void createResult(Collector<MergeTriplet> out, MergeTriplet triplet) {
    if (!AbstractionUtils.hasOverlap(
        triplet.getSrcTuple().getIntSources(),
        triplet.getTrgTuple().getIntSources())) {
      out.collect(triplet);
    }
  }
}