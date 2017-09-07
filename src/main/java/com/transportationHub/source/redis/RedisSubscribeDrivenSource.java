package com.transportationHub.source.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.util.SafeEncoder;
import redis.clients.jedis.exceptions.JedisConnectionException;
import com.google.common.base.Preconditions;

public class RedisSubscribeDrivenSource extends AbstractRedisSource implements EventDrivenSource {

  private static final Logger LOG = LoggerFactory.getLogger(RedisSubscribeDrivenSource.class);

  private byte[][] redisChannels;
  private boolean runFlag;

  @Override
  public void configure(Context context) {
    String redisChannel = context.getString("redisChannel");
    Preconditions.checkNotNull(redisChannel, "Redis Channel must be set.");
    redisChannels = SafeEncoder.encodeMany(redisChannel.split(","));

    super.configure(context);
    LOG.info("Flume Redis Subscribe Source Configured");
  }

  @Override
  public synchronized void start() {
    super.start();

    runFlag = true;
    new Thread(new SubscribeManager()).start();
  }

  @Override
  public synchronized void stop() {
    super.stop();

    runFlag = false;

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private class SubscribeManager implements Runnable {

    private Thread subscribeRunner;
    private BinaryJedisPubSub jedisPubSub;

    @Override
    public void run() {
      LOG.info("Subscribe Manager Thread is started.");

      jedisPubSub = new JedisSubscribeListener();
      subscribeRunner = new Thread(new SubscribeRunner(jedisPubSub));

      subscribeRunner.start();

      while (runFlag) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      // TODO: think better way for thread safety.
      jedisPubSub.unsubscribe(redisChannels);
    }
  }

  private class SubscribeRunner implements Runnable {

    private BinaryJedisPubSub jedisPubSub;

    public SubscribeRunner(BinaryJedisPubSub jedisPubSub) {
      LOG.info("Subscribe Runner Thread is started.");

      this.jedisPubSub = jedisPubSub;
    }

    @Override
    public void run() {
      while (runFlag) {
        try {
          jedis.subscribe(jedisPubSub, redisChannels);
        } catch (JedisConnectionException e) {
          LOG.error("Disconnected from Redis...");
          connect();
        } catch (Exception e) {
          LOG.error("FATAL ERROR: unexpected exception in SubscribeRunner.", e);
        }
      }
    }
  }

  private class JedisSubscribeListener extends BinaryJedisPubSub{

    @Override
    public void onMessage(byte[] channel, byte[] message) {
      try {
        channelProcessor.processEvent(messageHandler.getEvent(message));
      } catch (Exception e) {
        LOG.error("RedisMessageHandler threw unexpected exception.", e);
      }
    }

    @Override
    public void onPMessage(byte[] pattern, byte[] channel, byte[] message) {
      // TODO: Pattern subscribe feature will be implemented.
    }

    @Override
    public void onSubscribe(byte[] channel, int subscribedChannels) {
      LOG.info("onSubscribe (Channel: " + channel + ")");
    }

    @Override
    public void onUnsubscribe(byte[] channel, int subscribedChannels) {
      LOG.info("onUnsubscribe (Channel: " + channel + ")");
    }

    @Override
    public void onPUnsubscribe(byte[] pattern, int subscribedChannels) {
      // TODO: Pattern subscribe feature will be implemented.
    }

    @Override
    public void onPSubscribe(byte[] pattern, int subscribedChannels) {
      // TODO: Pattern subscribe feature will be implemented.
    }
  }
}
