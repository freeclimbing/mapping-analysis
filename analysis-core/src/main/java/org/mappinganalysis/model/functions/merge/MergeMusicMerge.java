package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.Set;

/**
 * Merge implementation for music dataset.
 *
 * TODO most attributes missing
 */
public class MergeMusicMerge
    implements FlatMapFunction<MergeMusicTriplet, MergeMusicTuple> {
  private static final Logger LOG = Logger.getLogger(MergeMusicMerge.class);

  @Override
  public void flatMap(MergeMusicTriplet triplet, Collector<MergeMusicTuple> out) throws Exception {
    MergeMusicTuple priority = triplet.getSrcTuple();
    MergeMusicTuple minor = triplet.getTrgTuple();

    Set<Long> trgElements = minor.getClusteredElements();
    Set<Long> srcElements = priority.getClusteredElements();
    if (srcElements.size() < trgElements.size()) {
      MergeMusicTuple tmp = minor;
      minor = priority;
      priority = tmp;
    }

    MergeMusicTuple mergedCluster = new MergeMusicTuple();
    mergedCluster.setId(priority.getId() > minor.getId() ? minor.getId() : priority.getId());

    srcElements.addAll(trgElements);
    mergedCluster.addClusteredElements(srcElements);

    mergedCluster.setIntSources(AbstractionUtils.mergeIntValues(
        priority.getIntSources(),
        minor.getIntSources()));

//    mergedCluster.setBlockingLabel(priority.getBlockingLabel());

    /**
     * test things
     */
    mergedCluster.setAttribute(Constants.BLOCKING_LABEL, priority, minor);
    mergedCluster.setAttribute(Constants.LABEL, priority, minor);
    mergedCluster.setAttribute(Constants.ALBUM, priority, minor);
    mergedCluster.setAttribute(Constants.ARTIST, priority, minor);
    mergedCluster.setAttribute(Constants.NUMBER, priority, minor);
//    mergedCluster.setAttribute(Constants.LANGUAGE, priority, minor);
    mergedCluster.setLang(Constants.EMPTY_STRING); // TODO check
    mergedCluster.setAttribute(Constants.YEAR, priority, minor);
    mergedCluster.setAttribute(Constants.LENGTH, priority, minor);

//    LOG.info("### new cluster: " + mergedCluster.toString());
    MergeMusicTuple fakeCluster = new MergeMusicTuple(
        priority.getId() > minor.getId() ? priority.getId() : minor.getId());
//    LOG.info("### fake cluster: " + fakeCluster.toString());

    LOG.info("fake: " + fakeCluster.toString());
    LOG.info("merged: " + mergedCluster.toString());

    out.collect(fakeCluster);
    out.collect(mergedCluster);
  }
}
