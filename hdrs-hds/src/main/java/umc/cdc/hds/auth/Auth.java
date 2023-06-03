/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.auth;

import umc.cdc.hds.core.Api;

/**
 *
 * @author brandboat
 */
public interface Auth {

  public boolean access(String token, Api api);
}
