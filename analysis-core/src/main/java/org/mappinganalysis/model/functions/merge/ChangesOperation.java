package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.MergeTuple;

/**
 * Create changed triplets within delta iteration operation.
 */
public class ChangesOperation<T>
    implements CustomUnaryOperation<MergeTriplet<T>, MergeTriplet<T>> {
  private DataSet<T> delta;
  private DataSet<Tuple2<Long, Long>> transitions;
  private DataDomain domain;
  private DataSet<MergeTriplet<T>> workset;

  public ChangesOperation(DataSet<T> delta,
                          DataSet<Tuple2<Long, Long>> transitions,
                          DataDomain domain) {
    this.delta = delta;
    this.transitions = transitions;
    this.domain = domain;
  }

  @Override
  public void setInput(DataSet<MergeTriplet<T>> inputData) {
    this.workset = inputData;
  }

  @Override
  public DataSet<MergeTriplet<T>> createResult() {
    DataSet<MergeTriplet<T>> leftChanges = workset.join(transitions)
        .where(0)
        .equalTo(0)
        .with(new TransitionJoinFunction<>(0))
        .distinct(0,1)
        .join(delta.filter(new ActiveFilterFunction<>(domain)))
        .where(0)
        .equalTo(0)
        .with(new TripletTupleJoinFunction<>(0));

    return workset.join(transitions)
        .where(1)
        .equalTo(0)
        .with(new TransitionJoinFunction<T>(1))
        .distinct(0,1)
        .join(delta.filter(new ActiveFilterFunction<>(domain)))
        .where(1)
        .equalTo(0)
        .with(new TripletTupleJoinFunction<>(1))
        .union(leftChanges);
  }


  private static class TransitionJoinFunction<T>
      implements JoinFunction<MergeTriplet<T>, Tuple2<Long, Long>, MergeTriplet<T>> {
    private Integer position;

    public TransitionJoinFunction(Integer position) {
      this.position = position;
    }

    @Override
    public MergeTriplet<T> join(
        MergeTriplet<T> triplet,
        Tuple2<Long, Long> transition) throws Exception {
      if (position == 0) {
        triplet.setSrcId(transition.f1);
      } else if (position == 1) {
        triplet.setTrgId(transition.f1);
      } else {
        throw new IllegalArgumentException("Unsupported position: " + position);
      }

      return triplet;
    }
  }

  private static class ActiveFilterFunction<T> implements FilterFunction<T> {
    private DataDomain domain;

    public ActiveFilterFunction(DataDomain domain) {
      this.domain = domain;
    }

    @Override
    public boolean filter(T value) throws Exception {
      if (domain == DataDomain.GEOGRAPHY) {
        MergeGeoTuple tuple = (MergeGeoTuple) value;
        return tuple.isActive();
      } else {
        MergeMusicTuple tuple = (MergeMusicTuple) value;
        return tuple.isActive();
      }
    }
  }

  private static class TripletTupleJoinFunction<T>
      implements JoinFunction<MergeTriplet<T>, T, MergeTriplet<T>> {
    private final Integer position;

    public TripletTupleJoinFunction(Integer position) {
      this.position = position;
    }

    @Override
    public MergeTriplet<T> join(MergeTriplet<T> triplet, T newTuple) throws Exception {
      if (position == 0) {
        triplet.setSrcTuple(newTuple);
      } else if (position == 1) {
        triplet.setTrgTuple(newTuple);
      } else {
        throw new IllegalArgumentException("Unsupported position: " + position);
      }
//          LOG.info("LEFT DELTA JOIN " + triplet.toString());
      return triplet;
    }
  }
}
