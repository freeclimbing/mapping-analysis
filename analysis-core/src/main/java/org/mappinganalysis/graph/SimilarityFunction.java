package org.mappinganalysis.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;

/**
 * Abstract class for all similarity functions.
 */
public abstract class SimilarityFunction<T>
    implements MapFunction<T, T> {
}
