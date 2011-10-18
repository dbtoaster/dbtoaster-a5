package org.dbtoaster.experiments.finance.events;

import java.util.Random;

public class UnifiedEvent {

  public final static long seed = 12345;
  final static Random rng = new Random(seed);

  private String streamName;
  private int eventType;
  private double timestamp;
  private int orderId;
  private int brokerId;
  private double volume;
  private double price;

  public UnifiedEvent() {}

  public UnifiedEvent(UnifiedEvent e) {
    streamName = e.streamName;
    eventType = e.eventType;
    timestamp = e.timestamp;
    orderId = e.orderId;
    brokerId = e.brokerId;
    volume = e.volume;
    price = e.price;
  }

  public UnifiedEvent(String n, int et, double t, int oid, int bid, double p, double v) {
    streamName = n;
    eventType = et;
    timestamp = t;
    orderId = oid;
    brokerId = bid;
    volume = v;
    price = p;
  }

  public String getStreamName() { return streamName; }
  public int getEventType() { return eventType; }
  public double getTimestamp() { return timestamp; }
  public int getOrderId() { return orderId; }
  public int getBrokerId() { return brokerId; }
  public double getVolume() { return volume; }
  public double getPrice() { return price; }

  public void setStreamName(String n) { streamName = n; }
  public void setEventType(int t) { eventType = t; }
  public void setTimestamp(double t) { timestamp = t; }
  public void setOrderId(int i) { orderId = i; }
  public void setBrokerId(int i) { brokerId = i; }
  public void setVolume(double v) { volume = v; }
  public void setPrice(double p) { price = p; }

  public String toString() {
    return streamName+","+eventType+","+timestamp+","+orderId+","+brokerId+","+volume+","+price;
  }
  
  public static UnifiedEvent randomEntry() {
    boolean ask = rng.nextDouble() < 0.5;
    boolean insert = rng.nextDouble() < 0.5;
    return new UnifiedEvent(
        (ask? "ASKS" : "BIDS"), (insert? 1 : 0),
        rng.nextDouble(), rng.nextInt(100), rng.nextInt(10),
        rng.nextInt(10), rng.nextInt(10));
  }
}
