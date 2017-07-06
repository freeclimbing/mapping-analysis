package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.util.AbstractionUtils;

/**
 */
public class CheckRestrictionsFilterFunction<T>
    implements FilterFunction<T> {
  private static final Logger LOG = Logger.getLogger(CheckRestrictionsFilterFunction.class);

  private DataDomain domain;
  private int sourcesCount;

  public CheckRestrictionsFilterFunction(DataDomain domain, int sourcesCount) {
    this.domain = domain;
    this.sourcesCount = sourcesCount;
  }

  @Override
  public boolean filter(T input) throws Exception {
//     LOG.info("CHANGED AND GETS NEW SIM " + input.toString());
    int srcIntSources;
    int trgIntSources;

    if (domain == DataDomain.GEOGRAPHY) {
      MergeGeoTriplet triplet = (MergeGeoTriplet) input;

      MergeGeoTuple src = triplet.getSrcTuple();
      srcIntSources = src.getIntSources();
      MergeGeoTuple trg = triplet.getTrgTuple();
      trgIntSources = trg.getIntSources();
    } else {
      MergeMusicTriplet triplet = (MergeMusicTriplet) input;

      MergeMusicTuple src = triplet.getSrcTuple();
      srcIntSources = src.getIntSources();
      MergeMusicTuple trg = triplet.getTrgTuple();
      trgIntSources = trg.getIntSources();
    }

    boolean hasSourceOverlap = AbstractionUtils.hasOverlap(srcIntSources, trgIntSources);
    boolean isSourceCountChecked = sourcesCount >=
        (AbstractionUtils.getSourceCount(srcIntSources)
            + AbstractionUtils.getSourceCount(trgIntSources));
    return !hasSourceOverlap && isSourceCountChecked;
  }
}
