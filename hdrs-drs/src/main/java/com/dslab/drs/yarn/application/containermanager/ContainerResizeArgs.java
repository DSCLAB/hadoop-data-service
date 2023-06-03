/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.drs.yarn.application.containermanager;

/**
 *
 * @author Weli
 */
public class ContainerResizeArgs {

  public int memory;
  public long number;
  public boolean isRelease;

  public ContainerResizeArgs(int mem, long num, boolean isRelease) {
    this.memory = mem;
    this.number = num;
    this.isRelease = isRelease;
  }

}
