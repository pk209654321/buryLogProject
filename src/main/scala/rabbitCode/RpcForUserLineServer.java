package rabbitCode;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import conf.ConfigurationManager;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RpcForUserLineServer {

    private static final String REQUEST_ROUTINGKEY = ConfigurationManager.getProperty("rabbit.routingKey");
    private static int index=0;
    //具体处理方法
    private static int fib(int n) {
        if (n == 0)
            return 0;
        if (n == 1)
            return 1;
        return fib(n - 1) + fib(n - 2);
    }

    public static void main(String[] argv) {
         //建立连接、通道，并声明队列 
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("121.196.192.154");
        factory.setUsername("admin");
        factory.setPassword("123456");
        factory.setVirtualHost("/");
        Connection connection = null;
        try {
            connection = factory.newConnection();
            final Channel channel = connection.createChannel();

            channel.queueDeclare(REQUEST_ROUTINGKEY, false, false, false, null);

            channel.basicQos(1);

            System.out.println(" [x] Awaiting RPC requests");

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                        byte[] body) throws IOException {
                    AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                            .correlationId(properties.getCorrelationId()).build();

                    String response = "";

                    try {
                        String message = new String(body, "UTF-8");
                        //int n = Integer.parseInt(message);

                        System.out.println(" [.] fib(" + message + ")");
                        index++;
                       // response += fib(n);
                        response=index+"";
                    } catch (RuntimeException e) {
                        System.out.println(" [.] " + e.toString());
                    } finally {
                        // 返回处理结果队列
                        channel.basicPublish("", properties.getReplyTo(), replyProps, response.getBytes("UTF-8"));
                        //  确认消息，已经收到后面参数 multiple：是否批量.true:将一次性确认所有小于envelope.getDeliveryTag()的消息。
                        channel.basicAck(envelope.getDeliveryTag(), false);
                        // RabbitMq consumer worker thread notifies the RPC
                        // server owner thread
                        synchronized (this) {
                            this.notify();
                        }
                    }
                }
            };
            //取消自动确认
            boolean autoAck = false ;
            channel.basicConsume(REQUEST_ROUTINGKEY, autoAck, consumer);
            // Wait and be prepared to consume the message from RPC client.
            while (true) {
                synchronized (consumer) {
                    try {
                        consumer.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        } finally {
            if (connection != null)
                try {
                    connection.close();
                } catch (IOException _ignore) {
                }
        }
    }
}
