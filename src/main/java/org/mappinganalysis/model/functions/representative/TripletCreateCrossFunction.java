package org.mappinganalysis.model.functions.representative;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;

public class TripletCreateCrossFunction implements CrossFunction<Vertex<Long, ObjectMap>,
    Vertex<Long, ObjectMap>, Triplet<Long, ObjectMap, NullValue>> {
  private final Triplet<Long, ObjectMap, NullValue> reuseTriplet;

  public TripletCreateCrossFunction() {
    reuseTriplet = new Triplet<>();
  }

  @Override
  public Triplet<Long, ObjectMap, NullValue> cross(Vertex<Long, ObjectMap> left, Vertex<Long, ObjectMap> right) throws Exception {
    if ((long) left.getId() == right.getId() || left.getId() > right.getId()) {
      reuseTriplet.setFields(0L, 0L, null, null, null);
      return reuseTriplet;
    } else {
      reuseTriplet.setFields(left.getId(), right.getId(), left.getValue(), right.getValue(), NullValue.getInstance());
      return reuseTriplet;
    }
  }
}
