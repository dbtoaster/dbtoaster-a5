package org.dbtoaster.experiments.finance.events;

import java.util.Random;

public class OrderBookEntry {
  
  public final static long seed = 12345;
  final static Random rng = new Random(seed);

  private double timestamp;
  private int orderId;
  private int brokerId;
  private double volume;
  private double price;
  
  public OrderBookEntry() {}

  public OrderBookEntry(OrderBookEntry e) {
    timestamp = e.timestamp;
    orderId = e.orderId;
    brokerId = e.brokerId;
    volume = e.volume;
    price = e.price;
  }

  public OrderBookEntry(double t, int oid, int bid, double p, double v) {
    timestamp = t;
    orderId = oid;
    brokerId = bid;
    volume = v;
    price = p;
  }

  public double getTimestamp() { return timestamp; }
  public int getOrderId() { return orderId; }
  public int getBrokerId() { return brokerId; }
  public double getVolume() { return volume; }
  public double getPrice() { return price; }

  public void setTimestamp(double t) { timestamp = t; }
  public void setOrderId(int i) { orderId = i; }
  public void setBrokerId(int i) { brokerId = i; }
  public void setVolume(double v) { volume = v; }
  public void setPrice(double p) { price = p; }

  public String toString() {
    return timestamp+","+orderId+","+brokerId+","+volume+","+price;
  }
  
  public static OrderBookEntry randomEntry() {
    return new OrderBookEntry(
        rng.nextDouble(), rng.nextInt(100), rng.nextInt(10),
        rng.nextInt(10), rng.nextInt(10));
  }
}
