/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.parser;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 *
 * @author brandboat
 * @param <T>
 */
public abstract class ElaboratedValue<T> {

  protected Attribute attribute;

  public ElaboratedValue(final String s) {
    if (s == null) {
      attribute = Attribute.NULL;
    } else if (s.isEmpty()) {
      attribute = Attribute.EMPTY;
    } else {
      attribute = Attribute.HAS_VALUE;
    }
  }

  public ElaboratedValue<T> ifNull(Runnable run) {
    if (attribute.equals(Attribute.NULL)) {
      run.run();
    }
    return this;
  }

  public ElaboratedValue<T> ifEmpty(Runnable run) {
    if (attribute.equals(Attribute.EMPTY)) {
      run.run();
    }
    return this;
  }

  public ElaboratedValue<T> ifHasValue(Consumer<? super T> consumer) {
    if (attribute.equals(Attribute.HAS_VALUE)) {
      consumer.accept(getValue());
    }
    return this;
  }

  public <U> Optional<U> map(Function<? super T, ? extends U> mapper) {
    Objects.requireNonNull(mapper);
    if (attribute.equals(Attribute.HAS_VALUE)) {
      return Optional.ofNullable(mapper.apply(getValue()));
    }
    return Optional.empty();
  }

  protected abstract T getValue();
}
