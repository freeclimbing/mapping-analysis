package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.Set;

/**
 * Merge function, for the "losing" tuple, add a dummy result tuple for solution set.
 *
 * Do not use reuse tuple.
 */
public class MergeGeoMapFunction<T>
    implements FlatMapFunction<MergeTriplet<T>, T> {
  private static final Logger LOG = Logger.getLogger(MergeGeoMapFunction.class);

  @Override
  public void flatMap(MergeTriplet<T> triplet, Collector<T> out) throws Exception {
    MergeGeoTuple priority = (MergeGeoTuple) triplet.getSrcTuple();
    MergeGeoTuple minor = (MergeGeoTuple) triplet.getTrgTuple();

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

//    LOG.info("### new cluster: " + mergedCluster.toString());
    MergeGeoTuple fakeCluster = new MergeGeoTuple(
        priority.getId() > minor.getId() ? priority.getId() : minor.getId());
//    LOG.info("### fake cluster: " + fakeCluster.toString());

    out.collect((T) fakeCluster);
    out.collect((T) mergedCluster);
  }
}
