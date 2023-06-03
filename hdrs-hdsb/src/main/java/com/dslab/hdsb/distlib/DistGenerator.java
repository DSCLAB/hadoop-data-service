package com.dslab.hdsb.distlib;

@FunctionalInterface
public interface DistGenerator<T> {
  T generate();
}
