package com.guokr.hebo.tap;

import java.util.HashMap;
import java.util.Map;

public enum Granularity {

  YEARLY(0), MONTHLY(1), DAILY(2), HOURLY(3), MINUTELY(4);

  private final int value;

  private Granularity(final int newValue) {
    value = newValue;
  }

  public int getValue() {
    return value;
  }

  public Granularity getPrevious() {
    return this.ordinal() != 0 ? Granularity.values()[this.ordinal() - 1]
        : null;
  }

  public String toField() {
    Map<Granularity, String> map = new HashMap<Granularity, String>() {{
      put(Granularity.YEARLY, "?year");
      put(Granularity.MONTHLY, "?month");
      put(Granularity.HOURLY, "?hour");
      put(Granularity.MINUTELY, "?minute");
    }};
    return map.get(this);
  }
}

