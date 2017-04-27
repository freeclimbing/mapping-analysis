package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FilterFunction;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.util.AbstractionUtils;

/**
 * Created by markus on 4/27/17.
 */
class CheckRestrictionsFilterFunction
    implements FilterFunction<MergeGeoTriplet> {
  private int sourcesCount;

  public CheckRestrictionsFilterFunction(int sourcesCount) {
    this.sourcesCount = sourcesCount;
  }

  @Override
  public boolean filter(MergeGeoTriplet triplet) throws Exception {
    // LOG.info("CHANGED AND GETS NEW SIM " + triplet.toString());
    int srcIntSources;
    int trgIntSources;

//      if (domain == DataDomain.GEOGRAPHY) {
    MergeGeoTuple src = (MergeGeoTuple) triplet.getSrcTuple();
    srcIntSources = src.getIntSources();
    MergeGeoTuple trg = (MergeGeoTuple) triplet.getTrgTuple();
    trgIntSources = trg.getIntSources();
//      } else {
//        MergeMusicTuple src = (MergeMusicTuple) triplet.getSrcTuple();
//        srcIntSources = src.getIntSources();
//        MergeMusicTuple trg = (MergeMusicTuple) triplet.getTrgTuple();
//        trgIntSources = trg.getIntSources();
//      }

    boolean hasSourceOverlap = AbstractionUtils.hasOverlap(srcIntSources, trgIntSources);
    boolean isSourceCountChecked = sourcesCount >=
        (AbstractionUtils.getSourceCount(srcIntSources)
            + AbstractionUtils.getSourceCount(trgIntSources));
    return !hasSourceOverlap && isSourceCountChecked;
  }
}
