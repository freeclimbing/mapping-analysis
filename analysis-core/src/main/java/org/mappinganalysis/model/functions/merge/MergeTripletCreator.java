package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.util.AbstractionUtils;

import java.util.HashSet;

/**
 * Create triplets for merge, properties are added after checking restrictions,
 * similarity is not added in this creation process.
 *
 * restrictions:
 * - type of two vertices have an overlap
 * - sources of two vertices do not overlap
 * - size of resulting cluster is not bigger than numbers of data sources
 *
 * Careful:
 * - MergeTriplets have some properties which contain a start value to
 * avoid null pointer exceptions.
 * - reuse objects side effects, don't use here
 * - smaller id is always srcTuple
 */
class MergeTripletCreator<T>
    implements GroupReduceFunction<T, MergeTriplet<T>> {
  private static final Logger LOG = Logger.getLogger(MergeTripletCreator.class);
  private DataDomain domain;
  private final int sourcesCount;

  public MergeTripletCreator(DataDomain domain, int sourcesCount) {
    this.domain = domain;
    this.sourcesCount = sourcesCount;
  }

  @Override
  public void reduce(Iterable<T> values,
                     Collector<MergeTriplet<T>> out) throws Exception {
    HashSet<T> leftSide = Sets.newHashSet(values);
    HashSet<T> rightSide = Sets.newHashSet(leftSide);

    if (domain == DataDomain.GEOGRAPHY) {
      for (T leftGenericTuple : leftSide) {
        MergeGeoTuple leftTuple = (MergeGeoTuple) leftGenericTuple;
        MergeTriplet<T> triplet = new MergeTriplet<>();
        Integer leftSources = leftTuple.getIntSources();
        Integer leftTypes = leftTuple.getIntTypes();

        triplet.setBlockingLabel(leftTuple.getBlockingLabel());
        triplet.setSimilarity(0D);
        rightSide.remove(leftGenericTuple);
        for (T rightGenericTuple : rightSide) {
          MergeGeoTuple rightTuple = (MergeGeoTuple) rightGenericTuple;
          int summedSources = AbstractionUtils.getSourceCount(leftSources)
              + AbstractionUtils.getSourceCount(rightTuple.getIntSources());

          if (summedSources <= sourcesCount
              && !AbstractionUtils.hasOverlap(leftSources, rightTuple.getIntSources())
              && AbstractionUtils.hasOverlap(leftTypes, rightTuple.getIntTypes())) {

            triplet.setIdAndTuples(leftGenericTuple, rightGenericTuple, domain);

          LOG.info(rightTuple.toString() + " ### " + leftTuple.toString());
          LOG.info(triplet.toString());
            out.collect(triplet);
          }
        }
      }
    } else if (domain == DataDomain.MUSIC) {
      for (T leftGenericTuple : leftSide) {
        MergeMusicTuple leftTuple = (MergeMusicTuple) leftGenericTuple;
        MergeTriplet<T> triplet = new MergeTriplet<>();
        Integer leftSources = leftTuple.getIntSources();

        triplet.setBlockingLabel(leftTuple.getBlockingLabel());
        rightSide.remove(leftGenericTuple);
        for (T rightGenericTuple : rightSide) {
          MergeMusicTuple rightTuple = (MergeMusicTuple) rightGenericTuple;
          int summedSources = AbstractionUtils.getSourceCount(leftSources)
              + AbstractionUtils.getSourceCount(rightTuple.getIntSources());

          if (summedSources <= sourcesCount
              && !AbstractionUtils.hasOverlap(leftSources, rightTuple.getIntSources())) {

            triplet.setIdAndTuples(leftGenericTuple, rightGenericTuple, domain);

//          LOG.info(rightTuple.toString() + " ### " + leftTuple.toString());
//          LOG.info(reuseTriplet.toString());
            out.collect(triplet);
          }
        }
      }
    }
  }
}
