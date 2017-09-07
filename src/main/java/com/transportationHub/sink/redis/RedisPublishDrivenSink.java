package com.transportationHub.sink.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.Event;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.EventDeliveryException;
import com.google.common.base.Preconditions;
import redis.clients.jedis.exceptions.JedisException;

public class RedisPublishDrivenSink extends AbstractRedisSink implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(RedisPublishDrivenSink.class);

  private byte[] redisChannel;

  @Override
  public void configure(Context context) {
    redisChannel = context.getString("redisChannel").getBytes();

    Preconditions.checkNotNull(redisChannel, "Redis Channel must be set.");

    super.configure(context);
    LOG.info("Flume Redis Publish Sink Configured");
  }

  @Override
  public synchronized void stop() {
    jedis.disconnect();
    super.stop();
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = null;

    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();

      Event event = channel.take();
      byte[] serialized = messageHandler.getBytes(event);

      if (jedis.publish(redisChannel, serialized) > 0) {
        transaction.commit();
        status = Status.READY;
      } else {
        throw new EventDeliveryException(
            "Event is published, but there is no receiver in this channel named " + redisChannel);
      }
    } catch (Throwable e) {
      transaction.rollback();
      status = Status.BACKOFF;

      // we need to rethrow jedis exceptions, because they signal that something went wrong
      // with the connection to the redis server
      if (e instanceof JedisException) {
        // TODO: we could try to reconnect and resend immediately
        jedis.disconnect();
        throw new EventDeliveryException(e);
      }

      if (e instanceof Error) {
        throw (Error) e;
      }
    } finally {
      transaction.close();
    }

    return status;
  }
}
