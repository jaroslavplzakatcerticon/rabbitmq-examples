package cz.certicon.jplzak.rabbitmq;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Uninterruptibles;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;

/**
 * @author jaroslav.plzak@certicon.cz
 *
 */
public class MessageReciever
{

   private static final Logger LOG = LoggerFactory.getLogger( MessageReciever.class.getSimpleName() );

   public static void main( final String[] args ) throws IOException, TimeoutException
   {

      // Create AMQP connection factory
      final ConnectionFactory factory = new ConnectionFactory();
      factory.setHost( Constants.AMQP_SERVER_ADDRESS );

      for ( int i = 0; i < 5; i++ )
      {
         LOG.info( "Connecting to {} ...", factory.getHost() );

         // Connect to RabbitMq server and create new channel (we will keep them open until the program is terminated from outside)
         final Connection connection = factory.newConnection();
         final Channel channel = connection.createChannel();

         LOG.info( "Connected", factory.getHost() );

         // declare queue in default exchange
         channel.queueDeclare( Constants.QUEUE_NAME, false, false, false, null );

         channel.basicQos( 1 );

         final boolean autoAck = false; // automatically acknowledge the received messages?

         // this will be consuming a as long as the channel is open (until program is terminated)
         final String consumerTag =
               channel.basicConsume( Constants.QUEUE_NAME, autoAck,
                     ( t, d ) -> messageReceivedCallback( t, d, channel ), MessageReciever::cancelledCallback );
         LOG.info( "{} Consuming from {}, waiting for messages ... ", consumerTag, Constants.QUEUE_NAME );
      }
   }


   private static void messageReceivedCallback( final String consumerTag, final Delivery delivery,
         final Channel channel )
      throws IOException
   {
      final String receivedMessage = new String( delivery.getBody(), "UTF-8" );
      LOG.info( "[x] {} Received {}", consumerTag, receivedMessage );

      final int napMillis = ThreadLocalRandom.current().nextInt( 1, 3000 );
      LOG.info( "{} Will be processing {} millis ....", consumerTag, napMillis );
      Uninterruptibles.sleepUninterruptibly( napMillis, TimeUnit.MILLISECONDS );

      // acknowledge the message was processed
      channel.basicAck( delivery.getEnvelope().getDeliveryTag(), false );

      LOG.info( "{} Done. Waiting for next message ... ", consumerTag );
   }


   private static void cancelledCallback( final String consumerTag )
   {
      LOG.info( "Consumer {} cancelled.", consumerTag );
   }
}
