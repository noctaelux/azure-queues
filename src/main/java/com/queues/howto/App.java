package com.queues.howto;

/**
 * Hello world!
 *
 */
public class App
{
    private static final String queueName = "azure-queue-name";

    private static final String connectStr =
            "DefaultEndpointsProtocol=https;" +
                    "AccountName=azure-storage-name;" +
                    "AccountKey=azure-account-key";

    public static void main( String[] args )
    {

        //AzureQueueSystem.createQueue(connectStr);

        //AzureQueueSystem.addQueueMessage(connectStr,queueName,"Hola Mundo desde Storage Queue (msg3)!");

        //AzureQueueSystem.peekQueueMessage(connectStr,queueName);

        //AzureQueueSystem.updateQueueMessage(connectStr,queueName,"msg2", "Hola mundo desde Storage Queue Modificado :v");

        //AzureQueueSystem.updateFirstQueueMessage(connectStr,queueName,"Se modificará el primer mensaje en el queue");

        //AzureQueueSystem.getQueueLength(connectStr,queueName);

        //AzureQueueSystem.dequeueMessage(connectStr,queueName);

        //AzureQueueSystem.listQueues(connectStr);

        //AzureQueueSystem.deleteMessageQueue(connectStr,queueName);

        System.out.println( "FIN" );
    }
}
