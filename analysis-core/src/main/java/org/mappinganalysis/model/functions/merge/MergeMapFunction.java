package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.Set;

/**
 * Merge function, for the "losing" tuple, add a dummy result tuple for solution set.
 *
 * Do not use reuse tuple.
 */
public class MergeMapFunction
    implements FlatMapFunction<MergeTriplet, MergeTuple> {
  private static final Logger LOG = Logger.getLogger(MergeMapFunction.class);

  @Override
  public void flatMap(MergeTriplet triplet, Collector<MergeTuple> out) throws Exception {
    MergeTuple priority = triplet.getSrcTuple();
    MergeTuple minor = triplet.getTrgTuple();

    Set<Long> trgElements = minor.getClusteredElements();
    Set<Long> srcElements = priority.getClusteredElements();
    if (srcElements.size() < trgElements.size()) {
      MergeTuple tmp = minor;
      minor = priority;
      priority = tmp;
    }

    MergeTuple mergedCluster = new MergeTuple();
    // set tuple properties
    mergedCluster.setId(priority.getId() > minor.getId() ? minor.getId() : priority.getId());
    // is there a case where minor label should be taken?
    mergedCluster.setLabel(priority.getLabel());

    // geo coordinates
    MergeTuple geoTuple = Utils.isOnlyOneValidGeoObject(priority, minor);
    if (geoTuple != null) {
      mergedCluster.setGeoProperties(geoTuple);
    } else if (AbstractionUtils.containsSrc(priority.getIntSources(), Constants.GN_NS)) {
      mergedCluster.setGeoProperties(priority);
    } else if (AbstractionUtils.containsSrc(minor.getIntSources(), Constants.GN_NS)) {
      mergedCluster.setGeoProperties(minor);
    } else if (AbstractionUtils.containsSrc(priority.getIntSources(), Constants.DBP_NS)) {
      mergedCluster.setGeoProperties(priority);
    } else if (AbstractionUtils.containsSrc(priority.getIntSources(), Constants.DBP_NS)) {
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
    MergeTuple fakeCluster = new MergeTuple(
        priority.getId() > minor.getId() ? priority.getId() : minor.getId(),
        false);
//    LOG.info("### fake cluster: " + fakeCluster.toString());

    out.collect(fakeCluster);
    out.collect(mergedCluster);
  }
}
