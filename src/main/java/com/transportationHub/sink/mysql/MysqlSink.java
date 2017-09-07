package com.transportationHub.sink.mysql;

/**
 * Created by MaJinZhou on 2017/9/7.
 */

import java.util.List;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

public class MysqlSink extends AbstractSink implements Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlSink.class);

    private String mysqlHost;
    private int mysqlPort;
    private String mysqlDatabaseName;
    private String mysqlTableName;
    private String mysqlUser;
    private String mysqlPassword;
    private int mysqlBatchSize;

    private PreparedStatement preparedStatement;
    private Connection conn;

    @Override
    public void configure(Context context) {
        mysqlHost = context.getString("host", "127.0.0.1");
        mysqlPort = context.getInteger("port", 3306);
        mysqlDatabaseName = context.getString("databaseName");
        mysqlTableName = context.getString("tableNam");
        mysqlUser = context.getString("user", "root");
        mysqlPassword = context.getString("password","123456");
        mysqlBatchSize = context.getInteger("batchSize", 100);
    }

    @Override
    public void start() {
        super.start();
        try {
            //调用Class.forName()方法加载驱动程序
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        String url = "jdbc:mysql://" + mysqlHost + ":" + mysqlPort + "/" + mysqlDatabaseName;
        //调用DriverManager对象的getConnection()方法，获得一个Connection对象

        try {
            conn = DriverManager.getConnection(url, mysqlUser, mysqlPassword);
            conn.setAutoCommit(false);
            //创建一个Statement对象
            preparedStatement = conn.prepareStatement("insert into " + mysqlTableName +
                    " (content) values (?)");

        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    @Override
    public void stop() {
        super.stop();
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        String content;

        List<String> actions = Lists.newArrayList();
        transaction.begin();
        try {
            for (int i = 0; i < mysqlBatchSize; i++) {
                event = channel.take();
                if (event != null) {
                    content = new String(event.getBody());
                    actions.add(content);
                } else {
                    result = Status.BACKOFF;
                    break;
                }
            }

            if (actions.size() > 0) {
                preparedStatement.clearBatch();
                for (String temp : actions) {
                    preparedStatement.setString(1, temp);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();

                conn.commit();
            }
            transaction.commit();
        } catch (Throwable e) {
            try {
                transaction.rollback();
            } catch (Exception e2) {
                LOG.error("Exception in rollback. Rollback might not have been" +
                        "successful.", e2);
            }
            LOG.error("Failed to commit transaction." +
                    "Transaction rolled back.", e);
            Throwables.propagate(e);
        } finally {
            transaction.close();
        }

        return result;
    }
}
