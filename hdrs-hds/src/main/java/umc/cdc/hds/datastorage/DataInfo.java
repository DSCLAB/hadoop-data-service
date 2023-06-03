package umc.cdc.hds.datastorage;

public interface DataInfo {

  public long getSize();

  /**
   * return the Name for the data.
   *
   * @return the Name for the data.
   */
  public String getName();

  /**
   * return the uri that specify the data.
   *
   * @return
   */
  public String getUri();
}
