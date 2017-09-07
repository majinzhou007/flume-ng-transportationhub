package com.transportationHub.handler;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

/**
 * RawHandler for RedisSource that accepts a string message and converts it
 * to a flume Event having no headers and a body set to message.
 */
public class RawHandler extends RedisMessageHandler {

  /**
   * {@inheritDoc}
   */
  public RawHandler(String charset) {
    super(charset);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Event getEvent(byte[] message) throws Exception {
    return EventBuilder.withBody(message);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] getBytes(Event event) throws Exception {
    return event.getBody();
  }
}
