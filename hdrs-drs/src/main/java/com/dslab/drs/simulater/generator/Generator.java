package com.dslab.drs.simulater.generator;

/**
 *
 * @author kh87313
 */
public abstract class Generator<V> {
  public abstract V nextValue();
  public abstract V lastValue();
}
