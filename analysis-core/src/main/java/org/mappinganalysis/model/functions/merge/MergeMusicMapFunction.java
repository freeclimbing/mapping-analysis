package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.util.AbstractionUtils;

import java.util.Set;

/**
 * Merge implementation for music dataset.
 *
 * TODO most attributes missing
 */
public class MergeMusicMapFunction<T>
    implements FlatMapFunction<MergeTriplet<T>, T> {
  private static final Logger LOG = Logger.getLogger(MergeMusicMapFunction.class);

  @Override
  public void flatMap(MergeTriplet<T> triplet, Collector<T> out) throws Exception {
    MergeMusicTuple priority = (MergeMusicTuple) triplet.getSrcTuple();
    MergeMusicTuple minor = (MergeMusicTuple) triplet.getTrgTuple();

    Set<Long> trgElements = minor.getClusteredElements();
    Set<Long> srcElements = priority.getClusteredElements();
    if (srcElements.size() < trgElements.size()) {
      MergeMusicTuple tmp = minor;
      minor = priority;
      priority = tmp;
    }

    MergeMusicTuple mergedCluster = new MergeMusicTuple();
    // set tuple properties
    mergedCluster.setId(priority.getId() > minor.getId() ? minor.getId() : priority.getId());
    // is there a case where minor label should be taken?
    mergedCluster.setLabel(priority.getLabel());

    srcElements.addAll(trgElements);
    mergedCluster.addClusteredElements(srcElements);
    mergedCluster.setIntSources(AbstractionUtils.mergeIntValues(
        priority.getIntSources(),
        minor.getIntSources()));
    mergedCluster.setBlockingLabel(priority.getBlockingLabel());

//    LOG.info("### new cluster: " + mergedCluster.toString());
    MergeMusicTuple fakeCluster = new MergeMusicTuple(
        priority.getId() > minor.getId() ? priority.getId() : minor.getId());
//    LOG.info("### fake cluster: " + fakeCluster.toString());

    out.collect((T) fakeCluster);
    out.collect((T) mergedCluster);
  }
}
