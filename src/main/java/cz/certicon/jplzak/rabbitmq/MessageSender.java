package cz.certicon.jplzak.rabbitmq;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author jaroslav.plzak@certicon.cz
 *
 */
public class MessageSender
{

   private static final int NUM_OF_MESSAGES = 100;

   private static final Logger LOG = LoggerFactory.getLogger( MessageSender.class.getSimpleName() );

   public static void main( final String[] args ) throws IOException, TimeoutException
   {

      // Create AMQP connection factory
      final ConnectionFactory factory = new ConnectionFactory();
      factory.setHost( Constants.AMQP_SERVER_ADDRESS );

      LOG.info( "Connecting to {} ...", factory.getHost() );

      // Connect to RabbitMq server and create new channel (both will be closed automatically when exiting the try-with-resources block
      try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel())
      {
         LOG.info( "Connected", factory.getHost() );

         // declare queue in default exchange
         channel.queueDeclare( Constants.QUEUE_NAME, false, false, false, null );

         LOG.info( "Starting publishing sequence of {} ", NUM_OF_MESSAGES );

         // sending loop:
         for ( int counter = 0; counter < NUM_OF_MESSAGES; counter++ )
         {
            final String message = String.format( Locale.ENGLISH, "Hello World message #%d", counter );
            channel.basicPublish( "", Constants.QUEUE_NAME, null, message.getBytes() );

            LOG.info( "[x] Sent {}", message );

            // Nap for random period before sending the next message
            //            final int napSeconds = ThreadLocalRandom.current().nextInt( 0, 5 );
            //            LOG.info( "sleeping {} seconds ....", napSeconds );
            //            Uninterruptibles.sleepUninterruptibly( napSeconds, TimeUnit.SECONDS );
         }
         LOG.info( "Publishing sequence finished. Closing." );
      }

      LOG.info( "Done." );
   }

}
