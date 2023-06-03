package com.dslab.drs.simulater.generator;

import java.util.Random;

/**
 *
 * @author kh87313
 */
public final class UniformIntegerGenerator extends Generator<Integer> {

  private Integer _lb, _ub, _interval, lastVal;
  private final Random rand;

  public UniformIntegerGenerator(int lowerBound, int upperBound) {
    setNewBound(lowerBound, upperBound);
    rand = new Random();
  }

  public void setNewBound(int lowerBound, int upperBound) {
    _lb = lowerBound;
    _ub = upperBound;
    _interval = _ub - _lb + 1;
    lastVal = -1;
  }

  @Override
  public Integer nextValue() {
    int ret = rand.nextInt(_interval) + _lb;
    setLastValue(ret);
    lastVal = ret;
    return ret;
  }

  @Override
  public Integer lastValue() {
    return lastVal;
  }

  private void setLastValue(Integer last) {
    lastVal = last;
  }
}
