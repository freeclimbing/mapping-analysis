package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.Set;

/**
 * Merge function, for the "losing" tuple, add a dummy result tuple for solution set.
 *
 * Do not use reuse tuple.
 */
public class DualMergeGeographyMapper
    implements FlatMapFunction<MergeGeoTriplet, MergeGeoTuple> {
  private static final Logger LOG = Logger.getLogger(DualMergeGeographyMapper.class);
  private boolean hasFakeResults;

  /**
   * Default constructor for delta iteration
   */
  public DualMergeGeographyMapper() {
    this.hasFakeResults = true;
  }

  /**
   * constructor for incremental clustering
   * @param hasFakeResults should be false, no fake results wanted
   */
  public DualMergeGeographyMapper(boolean hasFakeResults) {
    this.hasFakeResults = hasFakeResults;
  }

  @Override
  public void flatMap(MergeGeoTriplet triplet, Collector<MergeGeoTuple> out) throws Exception {
    if (triplet.getSrcId() == triplet.getTrgId().longValue()) {
      out.collect(triplet.getSrcTuple());
      return;
    }
    MergeGeoTuple priority = triplet.getSrcTuple();
    MergeGeoTuple minor = triplet.getTrgTuple();

    Set<Long> trgElements = minor.getClusteredElements();
    Set<Long> srcElements = priority.getClusteredElements();
    if (srcElements.size() < trgElements.size()) {
      MergeGeoTuple tmp = minor;
      minor = priority;
      priority = tmp;
    }

    MergeGeoTuple mergedCluster = new MergeGeoTuple();
    // set tuple properties
    mergedCluster.setId(priority.getId() > minor.getId() ? minor.getId() : priority.getId());
    // is there a case where minor label should be taken?
    mergedCluster.setLabel(priority.getLabel());

    // geo coordinates
    MergeGeoTuple geoTuple = Utils.isOnlyOneValidGeoObject(priority, minor);
    if (geoTuple != null) {
      mergedCluster.setGeoProperties(geoTuple);
    } else if (AbstractionUtils.containsSrc(Constants.GEO, priority.getIntSources(), Constants.GN_NS)) {
      mergedCluster.setGeoProperties(priority);
    } else if (AbstractionUtils.containsSrc(Constants.GEO, minor.getIntSources(), Constants.GN_NS)) {
      mergedCluster.setGeoProperties(minor);
    } else if (AbstractionUtils.containsSrc(Constants.GEO, priority.getIntSources(), Constants.DBP_NS)) {
      mergedCluster.setGeoProperties(priority);
    } else if (AbstractionUtils.containsSrc(Constants.GEO, priority.getIntSources(), Constants.DBP_NS)) {
      mergedCluster.setGeoProperties(minor);
    }

    srcElements.addAll(trgElements);
    mergedCluster.addClusteredElements(srcElements);
    mergedCluster.setIntSources(AbstractionUtils.mergeIntValues(
        priority.getIntSources(),
        minor.getIntSources()));
    mergedCluster.setIntTypes(AbstractionUtils.mergeIntValues(
        priority.getIntTypes(),
        minor.getIntTypes()));
    mergedCluster.setBlockingLabel(priority.getBlockingLabel());
    
    if (hasFakeResults) {
      MergeGeoTuple fakeCluster = new MergeGeoTuple(
          priority.getId() > minor.getId() ? priority.getId() : minor.getId());
//    LOG.info("### fake cluster: " + fakeCluster.toString());

      out.collect(fakeCluster);
    }
//    LOG.info("### new cluster: " + mergedCluster.toString());
    out.collect(mergedCluster);
  }
}
