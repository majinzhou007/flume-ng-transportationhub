package com.transportationHub.sink.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import redis.clients.jedis.BinaryJedis;
import com.google.common.base.Throwables;
import com.transportationHub.handler.RedisMessageHandler;

public abstract class AbstractRedisSink extends AbstractSink implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRedisSink.class);

  protected BinaryJedis jedis;
  protected RedisMessageHandler messageHandler;

  private String redisHost;
  private int redisPort;
  private int redisTimeout;
  private String redisPassword;

  @Override
  public void configure(Context context) {
    redisHost = context.getString("redisHost", "localhost");
    redisPort = context.getInteger("redisPort", 6379);
    redisTimeout = context.getInteger("redisTimeout", 2000);
    redisPassword = context.getString("redisPassword", "");

    try {
      String charset = context.getString("messageCharset", "utf-8");
      String handlerClassName = context.getString("handler", "com.transportationHub.handler.RawHandler");
      @SuppressWarnings("unchecked")
      Class<? extends RedisMessageHandler> clazz = (Class<? extends RedisMessageHandler>) Class.forName(handlerClassName);
      messageHandler = clazz.getDeclaredConstructor(String.class).newInstance(charset);
    } catch (ClassNotFoundException ex) {
      LOG.error("Error while configuring RedisMessageHandler. Exception follows.", ex);
      Throwables.propagate(ex);
    } catch (ClassCastException ex) {
      LOG.error("Handler is not an instance of RedisMessageHandler. Handler must implement RedisMessageHandler.");
      Throwables.propagate(ex);
    } catch (Exception ex) {
      LOG.error("Error configuring RedisSubscribeDrivenSource!", ex);
      Throwables.propagate(ex);
    }
  }

  @Override
  public synchronized void start() {
    jedis = new BinaryJedis(redisHost, redisPort, redisTimeout);
    if (!"".equals(redisPassword)) {
      jedis.auth(redisPassword);
    }

    // try to connect here already to find out about problems early on
    // TODO: we may need to throw a special kind of exception here
    jedis.connect();
    super.start();

    LOG.info("Redis Connected. (host: " + redisHost + ", port: " + String.valueOf(redisPort)
        + ", timeout: " + String.valueOf(redisTimeout) + ")");
  }

  @Override
  public synchronized void stop() {
    jedis.disconnect();
    super.stop();
  }

}
