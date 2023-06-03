/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.lock;

import umc.udp.core.framework.AtomicCloseable;

/**
 *
 * @author jpopaholic
 */
public abstract class Lock extends AtomicCloseable {

  public abstract byte[] getKey();
}
