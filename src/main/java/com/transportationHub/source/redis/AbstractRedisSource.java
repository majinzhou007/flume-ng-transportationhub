package com.transportationHub.source.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.channel.ChannelProcessor;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import com.google.common.base.Throwables;
import com.transportationHub.handler.RedisMessageHandler;


public class AbstractRedisSource extends AbstractSource implements Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractRedisSource.class);

    protected BinaryJedis jedis;
    protected ChannelProcessor channelProcessor;
    protected RedisMessageHandler messageHandler;

    private int redisPort;
    private int redisTimeout;
    private String redisHost;
    private String redisPassword;

    public void configure(Context context) {
        redisHost = context.getString("redisHost", "localhost");
        redisPort = context.getInteger("redisPort", 6379);
        redisTimeout = context.getInteger("redisTimeout", 2000);
        redisPassword = context.getString("redisPassword", "");

        try {
            String charset = context.getString("messageCharset", "utf-8");
            String handlerClassName = context.getString("handler", "com.transportationHub.source.redis.handler.RawHandler");
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

    protected void connect(){
        LOG.info("Redis Connecting");
        while (true) {
            try {
                jedis = new BinaryJedis(redisHost, redisPort, redisTimeout);
                if (!"".equals(redisPassword)) {
                    jedis.auth(redisPassword);
                } else {
                    jedis.ping();
                }
                break;
            } catch (JedisConnectionException e) {
                LOG.error("Connection failed.", e);
                LOG.info("Waiting for 10 seconds.");
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e1){
                    LOG.error("Waiting for 10 seconds fail.");
                }
            }
        }
        LOG.info("Redis Connected. (host: " + redisHost + ", port: " + String.valueOf(redisPort)
                 + ", timeout: " + String.valueOf(redisTimeout) + ")");
    }

    @Override
    public synchronized void start() {
        super.start();
        channelProcessor = getChannelProcessor();
        connect();
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }

}
