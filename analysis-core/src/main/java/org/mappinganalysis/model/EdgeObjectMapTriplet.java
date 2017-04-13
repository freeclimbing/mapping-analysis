package org.mappinganalysis.model;

import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.api.SimilarityOperation;

/**
 * Sim computation triplet with edge ObjectMap value (via SimilarityOperations)
 */
public class EdgeObjectMapTriplet extends Triplet<Long, ObjectMap, ObjectMap> {
  EdgeObjectMapTriplet triplet = null;

  public EdgeObjectMapTriplet(Triplet<Long, ObjectMap, NullValue> input) {
    this.f0 = input.f0;
    this.f1 = input.f1;
    this.f2 = input.f2;
    this.f3 = input.f3;
    this.f4 = new ObjectMap();
  }

  public EdgeObjectMapTriplet runOperation(
      SimilarityOperation<EdgeObjectMapTriplet> operation) {
		operation.setInput(this);
		return operation.createResult();
	}
}
