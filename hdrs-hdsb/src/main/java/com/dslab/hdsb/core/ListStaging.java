/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Weli
 */
public class ListStaging {

  private final List<String> list;

  public ListStaging() {
    this.list = new ArrayList<>();
  }

  public Iterator<String> getIterator() {
    return list.iterator();
  }

  public void setList(String e) {
    list.add(e);
  }

  public int getSize() {
    return list.size();
  }

  public Boolean isEmpty() {
    return list.isEmpty();
  }
}
