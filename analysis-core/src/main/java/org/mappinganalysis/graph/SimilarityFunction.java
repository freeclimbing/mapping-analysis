package org.mappinganalysis.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;

/**
 * Abstract class for all similarity functions.
 * @deprecated
 */
public abstract class SimilarityFunction<OUT> implements
    MapFunction<Triplet<Long, ObjectMap, NullValue>, OUT> {


}