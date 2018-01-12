package org.mappinganalysis.model.functions.blocking.lsh.utils;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;

public class SwitchMapFunction implements MapFunction<MergeGeoTriplet, MergeGeoTriplet> {
  private String newSource;

  public SwitchMapFunction(String newSource) {
    this.newSource = newSource;
  }

  @Override
  public MergeGeoTriplet map(MergeGeoTriplet triplet) throws Exception {
    int newSourceInt = AbstractionUtils
        .getSourcesInt(Constants.GEO, Sets.newHashSet(newSource));

    if (AbstractionUtils.hasOverlap(
        triplet.getSrcTuple().getIntSources(), newSourceInt)) {
      MergeGeoTuple tmp = triplet.getSrcTuple();
      triplet.setSrcTuple(triplet.getTrgTuple());
      triplet.setSrcId(triplet.getTrgId());

      triplet.setTrgTuple(tmp);
      triplet.setTrgId(tmp.getId());
    }

    return triplet;
  }
}
