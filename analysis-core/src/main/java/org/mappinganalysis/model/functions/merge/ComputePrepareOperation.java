package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.util.AbstractionUtils;

/**
 * Created by markus on 4/26/17.
 */
public class ComputePrepareOperation<T> implements CustomUnaryOperation<MergeTriplet<T>, MergeTriplet<T>> {
  private DataSet<MergeTriplet<T>> triplets;
  private DataDomain domain;
  private int sourcesCount;

  public ComputePrepareOperation(DataDomain domain, int sourcesCount) {
    this.domain = domain;
    this.sourcesCount = sourcesCount;
  }

  @Override
  public void setInput(DataSet<MergeTriplet<T>> inputData) {
    this.triplets = inputData;
  }

  @Override
  public DataSet<MergeTriplet<T>> createResult() {
    return triplets
        .map(new SortMapFunction<>())
        .distinct(0,1) // needed
        .filter(new CheckRestrictionsFilterFunction<>());
  }

  private class SortMapFunction<M>
      implements MapFunction<MergeTriplet<M>, MergeTriplet<M>> {
    @Override
    public MergeTriplet<M> map(MergeTriplet<M> triplet) throws Exception {
      if (triplet.getSrcId() > triplet.getTrgId()) {
        Object tmp = triplet.getSrcTuple();
        triplet.setSrcId(triplet.getTrgId());
        triplet.setSrcTuple(triplet.getTrgTuple());
        if (domain == DataDomain.GEOGRAPHY) {
          MergeGeoTuple tmpTuple = (MergeGeoTuple) tmp;
          triplet.setTrgId(tmpTuple.getId());
          triplet.setTrgTuple((M) tmpTuple);
        } else {
          MergeMusicTuple tmpTuple = (MergeMusicTuple) tmp;
          triplet.setTrgId(tmpTuple.getId());
          triplet.setTrgTuple((M) tmpTuple);
        }
      }
      return triplet;
    }
  }

  private class CheckRestrictionsFilterFunction<N>
      implements FilterFunction<MergeTriplet<N>> {
    @Override
    public boolean filter(MergeTriplet<N> triplet) throws Exception {
      // LOG.info("CHANGED AND GETS NEW SIM " + triplet.toString());
      int srcIntSources;
      int trgIntSources;

      if (domain == DataDomain.GEOGRAPHY) {
        MergeGeoTuple src = (MergeGeoTuple) triplet.getSrcTuple();
        srcIntSources = src.getIntSources();
        MergeGeoTuple trg = (MergeGeoTuple) triplet.getTrgTuple();
        trgIntSources = trg.getIntSources();
      } else {
        MergeMusicTuple src = (MergeMusicTuple) triplet.getSrcTuple();
        srcIntSources = src.getIntSources();
        MergeMusicTuple trg = (MergeMusicTuple) triplet.getTrgTuple();
        trgIntSources = trg.getIntSources();
      }

      boolean hasSourceOverlap = AbstractionUtils.hasOverlap(srcIntSources, trgIntSources);
      boolean isSourceCountChecked = sourcesCount >=
          (AbstractionUtils.getSourceCount(srcIntSources)
              + AbstractionUtils.getSourceCount(trgIntSources));
      return !hasSourceOverlap && isSourceCountChecked;
    }
  }
}
