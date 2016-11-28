package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Basic link filter
 */
public class BasicLinkFilter extends LinkFilter {
  BasicLinkFilter(Boolean removeIsolatedVertices,
                  ExecutionEnvironment env) {
    super(new BasicLinkFilterFunction(removeIsolatedVertices, env));
  }
}
