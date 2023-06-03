/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.core;

import java.util.Optional;

/**
 *
 * @author brandboat
 */
public enum Api {
  access,
  delete,
  batchdelete,
  list,
  list2,
  addmapping,
  deletemapping,
  listmapping,
  watch,
  run,
  loading,
  kill,
  killdrs,
  watchdrs;

  public static Optional<Api> find(String apiName) {
    if (apiName == null) {
      return Optional.empty();
    }
    for (Api api : Api.values()) {
      if (api.name().equalsIgnoreCase(apiName)) {
        return Optional.of(api);
      }
    }
    return Optional.empty();
  }
}
