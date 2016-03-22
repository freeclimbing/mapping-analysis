package org.mappinganalysis.model.functions.refinement;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

import java.util.Set;

/**
 * Create triplets for potentially matching vertices,
 * skip those where prerequisites are not good enough.
 *
 * Fix size 4 for big dataset. TODO
 */
public class TripletCreateCrossFunction implements CrossFunction<Vertex<Long, ObjectMap>,
    Vertex<Long, ObjectMap>, Triplet<Long, ObjectMap, NullValue>> {
  private static final Logger LOG = Logger.getLogger(TripletCreateCrossFunction.class);
  private final Triplet<Long, ObjectMap, NullValue> reuseTriplet;

  public TripletCreateCrossFunction() {
    reuseTriplet = new Triplet<>();
  }

  @Override
  public Triplet<Long, ObjectMap, NullValue> cross(Vertex<Long, ObjectMap> left, Vertex<Long, ObjectMap> right)
      throws Exception {
    // exclude if right and left contains same ontology somewhere
    Set<String> srcOnts = (Set<String>) left.getValue().get(Utils.ONTOLOGIES);
    Set<String> trgOnts = (Set<String>) right.getValue().get(Utils.ONTOLOGIES);
    for (String srcValue : srcOnts) {
      if (trgOnts.contains(srcValue)) {
        reuseTriplet.setFields(0L, 0L, null, null, null);
        return reuseTriplet;
      }
    }

    // exclude equal id and one of (1, 2) >>(2,1)<<
    if ((long) left.getId() == right.getId() || left.getId() > right.getId()) {
      reuseTriplet.setFields(0L, 0L, null, null, null);
      return reuseTriplet;
    } else if (left.getValue().getVerticesList().size() + right.getValue().getVerticesList().size() <= 4) {
      reuseTriplet.setFields(left.getId(), right.getId(), left.getValue(), right.getValue(), NullValue.getInstance());
      return reuseTriplet;
    } else {
      LOG.error("triplet size too high, skipped: " + left.getId() + " ### " + right.getId());
      reuseTriplet.setFields(0L, 0L, null, null, null);
      return reuseTriplet;
    }
  }
}