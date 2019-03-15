package rabbitCode;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import conf.ConfigurationManager;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class RpcForUserLineClient {
    private Connection connection;
    private Channel channel;
    private String replyQueueName;
    private static final String REQUEST_ROUTINGKEY = ConfigurationManager.getProperty("rabbit.routingKey");
    private static final String EXCHANGE_NAME = ConfigurationManager.getProperty("rabbit.exchange.name");
    private static final String RABBIT_IP = ConfigurationManager.getProperty("rabbit.ip");
    private static final Integer RABBIT_PORT = ConfigurationManager.getInteger("rabbit.port");
    private static final String RABBIT_USERNAME = ConfigurationManager.getProperty("rabbit.username");
    private static final String RABBIT_PASSWD = ConfigurationManager.getProperty("rabbit.passwd");
    private static final String RABBIT_VIRTUALHOST = ConfigurationManager.getProperty("rabbit.virtualhost");
    public RpcForUserLineClient() throws IOException, TimeoutException {
        //建立一个连接和一个通道，并为回调声明一个唯一的'回调'队列
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT_IP);
        factory.setUsername(RABBIT_USERNAME);
        factory.setPassword(RABBIT_PASSWD);
        factory.setVirtualHost(RABBIT_VIRTUALHOST);
        connection = factory.newConnection();
        channel = connection.createChannel();
        //定义一个临时变量的接受队列名
        replyQueueName = channel.queueDeclare().getQueue();
    }
    //发送RPC请求
    public void call(String message) throws IOException, InterruptedException {
        //生成一个唯一的字符串作为回调队列的编号
        String corrId = UUID.randomUUID().toString();
        //发送请求消息，消息使用了两个属性：replyto和correlationId
        //服务端根据replyto返回结果，客户端根据correlationId判断响应是不是给自己的
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().correlationId(corrId).replyTo(replyQueueName)
                .build();
        System.out.println("replyQueueName==============================================="+replyQueueName);
        //发布一个消息，requestQueueName路由规则
        channel.basicPublish("", REQUEST_ROUTINGKEY, props, message.getBytes("UTF-8"));

        //由于我们的消费者交易处理是在单独的线程中进行的，因此我们需要在响应到达之前暂停主线程。
        //这里我们创建的 容量为1的阻塞队列ArrayBlockingQueue，因为我们只需要等待一个响应。
       // final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);

        // String basicConsume(String queue, boolean autoAck, Consumer callback)
        channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                //检查它的correlationId是否是我们所要找的那个
                if (properties.getCorrelationId().equals(corrId)) {
                    //如果是，则响应BlockingQueue
                    //response.offer(new String(body, "UTF-8"));
                    System.out.println("client接受到了server端的数据"+new String(body, "UTF-8"));
                }
            }
        });

        //return response.take();
    }

    public void close() throws IOException {
        connection.close();
    }

    public static void main(String[] argv) {
        RpcForUserLineClient fibonacciRpc = null;
        String response = null;
        try {
            fibonacciRpc = new RpcForUserLineClient();

            System.out.println(" [x] Requesting fib(30)");
            fibonacciRpc.call("30");
            //System.out.println(" [.] Got '" + response + "'");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fibonacciRpc != null) {
                try {
                    fibonacciRpc.close();
                } catch (IOException _ignore) {
                }
            }
        }
    }
}
