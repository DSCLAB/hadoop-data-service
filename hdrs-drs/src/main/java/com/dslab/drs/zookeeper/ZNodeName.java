package com.dslab.drs.zookeeper;

public class ZNodeName implements Comparable<ZNodeName> {

  private final String name;
  private String prefix;
  private int sequence = -1;

  public ZNodeName(String name) {
    if (name == null) {
      throw new NullPointerException("ZNode Name can't be null");
    }
    this.name = name;
    this.prefix = name;
    int idx = name.lastIndexOf('-');
    if (idx >= 0) {
      this.prefix = name.substring(0, idx);
      try {
        this.sequence = Integer.parseInt(name.substring(idx + 1));
        // If an exception occurred we misdetected a sequence suffix,
        // so return -1.
      } catch (NumberFormatException e) {
      } catch (ArrayIndexOutOfBoundsException e) {
      }
    }
  }

  public String toString() {
    return name;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ZNodeName sequence = (ZNodeName) o;

    if (!name.equals(sequence.name)) {
      return false;
    }

    return true;
  }

  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public int compareTo(ZNodeName o) {
    int s1 = this.sequence;
    int s2 = o.sequence;
    if (s1 == -1 && s2 == -1) {
      return this.name.compareTo(o.name);
    }
    if (s1 == -1) {
      return -1;
    } else if (s2 == -1) {
      return 1;
    } else {
      return s1 - s2;
    }
  }

  public String getName() {
    return this.name;
  }

  public int getZNodeName() {
    return this.sequence;
  }

  public String getPrefix() {
    return this.prefix;
  }
}
