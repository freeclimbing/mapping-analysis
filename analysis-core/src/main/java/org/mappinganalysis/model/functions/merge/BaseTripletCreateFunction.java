package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.GeoDistance;

import java.math.BigDecimal;
import java.util.HashSet;

/**
 * Create base triplets for merge process, actual properties are added after checking restrictions
 */
class BaseTripletCreateFunction
    implements GroupReduceFunction<MergeTriplet, MergeTriplet> {
  private static final Logger LOG = Logger.getLogger(BaseTripletCreateFunction.class);

  private final MergeTriplet reuseTriplet;
  private final int sourcesCount;

  public BaseTripletCreateFunction(int sourcesCount) {
    this.sourcesCount = sourcesCount;
    this.reuseTriplet = new MergeTriplet();
  }

  @Override
  public void reduce(Iterable<MergeTriplet> values,
                     Collector<MergeTriplet> out) throws Exception {
    HashSet<MergeTriplet> leftSide = Sets.newHashSet(values);
    HashSet<MergeTriplet> rightSide = Sets.newHashSet(leftSide);

    for (MergeTriplet leftTuple : leftSide) {
      Integer leftSources = leftTuple.getIntSources();
      Integer leftTypes = leftTuple.getIntTypes();

      rightSide.remove(leftTuple);
      for (MergeTriplet rightTuple : rightSide) {
        int summedSources = AbstractionUtils.getSourceCount(leftSources)
            + AbstractionUtils.getSourceCount(rightTuple.getIntSources());

        if (summedSources <= sourcesCount
            && !AbstractionUtils.hasOverlap(leftSources, rightTuple.getIntSources())
            && AbstractionUtils.hasOverlap(leftTypes, rightTuple.getIntTypes())) {

          Long resultId = leftTuple.getId() < rightTuple.getId()
              ? leftTuple.getId() : rightTuple.getId();
          reuseTriplet.setId(resultId);

          int intTypes = AbstractionUtils.mergeIntValues(leftTypes, rightTuple.getIntTypes());
          reuseTriplet.setIntTypes(intTypes);

          int intSources = AbstractionUtils.mergeIntValues(leftSources, rightTuple.getIntSources());
          reuseTriplet.setIntSources(intSources);

//          reuseTriplet.setGeoCoordinates(leftTuple.getLatitude(),
//              leftTuple.getLongitude(),
//              rightTuple.getLatitude(),
//              rightTuple.getLongitude());

          LOG.info("### baseTriplet: " + reuseTriplet.toString());

          out.collect(reuseTriplet);
        }
      }
    }
  }
}
