package org.mappinganalysis.io.debug;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.log4j.Logger;

/**
 * Basic debug printer
 * TODO RichMapFunction may not be needed
 * @param <IN> input type
 */
public abstract class Printer<IN> extends RichMapFunction<IN, IN> {
  /**
   * String is put in front of debug output.
   */
  protected final String prefix;

  /**
   * Used to differ between iterative and non-iterative runtime context.
   */
  protected final boolean isIterative;

  /**
   * Constructor
   */
  public Printer() {
    this(false, "");
  }

  /**
   * Constructor
   *
   * @param isIterative true, if called in iterative context
   * @param prefix prefix for output
   */
  public Printer(boolean isIterative, String prefix) {
    this.isIterative  = isIterative;
    this.prefix       = prefix;
  }

  @Override
  public IN map(IN in) throws Exception {
    getLogger().debug(String.format("%s%s", getHeader(), getDebugString(in)));
    return in;
  }

  /**
   * Returns the debug string representation of the concrete Object.
   *
   * @param in input object
   * @return debug string representation for input object
   */
  protected abstract String getDebugString(IN in);

  /**
   * Returns the logger for the concrete subclass.
   *
   * @return logger
   */
  protected abstract Logger getLogger();

  /**
   * Builds the header for debug string which contains the prefix and the
   * superstep number (0 if non-iterative).
   *
   * @return debug header
   */
  protected String getHeader() {
    return String.format("[%d][%s]: ",
        isIterative ? getIterationRuntimeContext().getSuperstepNumber() : 0,
        prefix != null && !prefix.isEmpty() ? prefix : " ");
  }

}
