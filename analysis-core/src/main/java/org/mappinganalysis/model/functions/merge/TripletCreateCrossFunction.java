package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;

import java.util.Set;

/**
 * Create triplets for potentially matching vertices,
 * skip those where prerequisites are not good enough.
 */
public class TripletCreateCrossFunction implements CrossFunction<Vertex<Long, ObjectMap>,
    Vertex<Long, ObjectMap>, Triplet<Long, ObjectMap, NullValue>> {
  private static final Logger LOG = Logger.getLogger(TripletCreateCrossFunction.class);
  private final Triplet<Long, ObjectMap, NullValue> reuseTriplet;
  private final int maxClusterSize;

  public TripletCreateCrossFunction(int maxClusterSize) {
    reuseTriplet = new Triplet<>();
    this.maxClusterSize = maxClusterSize;
  }

  @Override
  public Triplet<Long, ObjectMap, NullValue> cross(Vertex<Long, ObjectMap> left, Vertex<Long, ObjectMap> right)
      throws Exception {
    // exclude if right and left contains same ontology somewhere
    Set<String> srcOnts = left.getValue().getOntologiesList();
    Set<String> trgOnts = right.getValue().getOntologiesList();
    for (String srcValue : srcOnts) {
      if (trgOnts.contains(srcValue)) {
        reuseTriplet.setFields(0L,
            0L,
            null,
            null,
            null);
        LOG.info(reuseTriplet.toString());
        return reuseTriplet;
      }
    }

    // exclude equal id and one of (1, 2) >>(2,1)<<
    // TODO check if this is correct
    if ((long) left.getId() == right.getId() || left.getId() > right.getId()) {
      reuseTriplet.setFields(0L, 0L, null, null, null);
      LOG.info(reuseTriplet.toString());

      return reuseTriplet;
    } else if (left.getValue().getVerticesList().size() + right.getValue().getVerticesList().size() <= maxClusterSize) {
      reuseTriplet.setFields(left.getId(),
          right.getId(),
          left.getValue(),
          right.getValue(),
          NullValue.getInstance());
      LOG.info(reuseTriplet.toString());

      return reuseTriplet;
    } else {
      LOG.error("triplet size too high, skipped: " + left.getId() + " ### " + right.getId());
      reuseTriplet.setFields(0L,
          0L,
          null,
          null,
          null);
      LOG.info(reuseTriplet.toString());

      return reuseTriplet;
    }
  }
}