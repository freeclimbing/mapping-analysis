package org.mappinganalysis.model;

import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.api.CustomOperation;

/**
 * Sim computation triplet with edge ObjectMap value (via SimilarityOperations)
 */
public class EdgeObjectMapTriplet extends Triplet<Long, ObjectMap, ObjectMap> {

//  private final Triplet<Long, ObjectMap, NullValue> input;

  public EdgeObjectMapTriplet(Triplet<Long, ObjectMap, NullValue> input) {
//    this.input = input;
    this.f0 = input.f0;
    this.f1 = input.f1;
    this.f2 = input.f2;
    this.f3 = input.f3;
    this.f4 = new ObjectMap(); // edge
  }

  // do we need this?
//  public EdgeObjectMapTriplet(Triplet<Long, ObjectMap, ObjectMap> input, Triplet<Long, ObjectMap, NullValue> input1) {
//    this.input = input1;
//    this.f0 = input.f0;
//    this.f1 = input.f1;
//    this.f2 = input.f2;
//    this.f3 = input.f3;
//    this.f4 = input.f4;
//  }

  public EdgeObjectMapTriplet runOperation(
      CustomOperation<EdgeObjectMapTriplet> operation) {
		operation.setInput(this);
		return operation.createResult();
	}
}
