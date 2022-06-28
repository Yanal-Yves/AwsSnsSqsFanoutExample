namespace AwsSnsSqsFanoutExample
{
  using System;
  using System.Collections.Generic;
  using System.Globalization;
  using System.IO;
  using System.Threading.Tasks;
  using Amazon.SimpleNotificationService;
  using Amazon.SimpleNotificationService.Model;
  using Amazon.SQS;
  using Amazon.SQS.Model;
  using Microsoft.Extensions.Configuration;

  /// <summary>
  /// This example :
  /// 1) Creates an AWS SNS Topic
  /// 2) Creates 2 AWS SQS queues
  /// 3) Subscribes the two queues to the SNS topic.
  /// 4) Publishes a message to the topic.
  /// 5) Reads from the the two queues.
  /// 6) Output the read messages to stdout.
  /// The code uses the AWS SDK for .NET and .NET Core 3.1.
  /// </summary>
  public class AwsSnsSqsFanoutExample
  {
    private static readonly IAmazonSimpleNotificationService ClientSns = new AmazonSimpleNotificationServiceClient();
    private static readonly IAmazonSQS ClientSqs = new AmazonSQSClient();

    public static async Task Main()
    {
      // Load appsettings.json
      var appSettings = LoadAppsettings();

      // Create the queues, the topic and the subscriptions
      CreateQueueResponse firstCreateQueueResponse = await CreateQueue(ClientSqs, appSettings.FirstQueueName, appSettings.KmsKeyId);
      Console.WriteLine($"The Queue {appSettings.FirstQueueName} with URL : {firstCreateQueueResponse.QueueUrl} was created");
      CreateQueueResponse secondCreateQueueResponse = await CreateQueue(ClientSqs, appSettings.SecondQueueName, appSettings.KmsKeyId);
      Console.WriteLine($"The Queue {appSettings.SecondQueueName} with URL : {secondCreateQueueResponse.QueueUrl} was created");
      string topicArn = await CreateTopic(appSettings.TopicName, appSettings.KmsKeyId);
      Console.WriteLine($"The topic {appSettings.TopicName} with ARN : {topicArn} was created");

      // Subscribe to the SQS Queue
      string firstSubscriptionArn = await SubscribeQueueAsync(appSettings.FirstQueueName, appSettings.TopicName);
      Console.WriteLine(firstSubscriptionArn);
      string secondSubscriptionArn = await SubscribeQueueAsync(appSettings.SecondQueueName, appSettings.TopicName);
      Console.WriteLine(secondSubscriptionArn);

      // Publish Message
      for (int i = 0; i < 5; i++)
      {
        string messageText = $"This is an example message number {i} to publish to the ExampleSNSTopic. {DateTime.UtcNow}";
        await PublishToTopicAsync(appSettings.TopicName, messageText);
      }

      // Receive messages from the queues
      ReceiveMessageResponse firstReceiveAndDeleteMessage = await ReceiveAndDeleteMessage(appSettings.FirstQueueName);
      Console.WriteLine($"{firstReceiveAndDeleteMessage.Messages.Count} messages received and deleted from the first queue.");
      ReceiveMessageResponse secondReceiveAndDeleteMessage = await ReceiveAndDeleteMessage(appSettings.SecondQueueName);
      Console.WriteLine($"{secondReceiveAndDeleteMessage.Messages.Count} messages received and deleted from the second queue.");
    }

    /// <summary>
    /// Creates a new Amazon SQS queue using the queue name passed to it
    /// in queueName.
    /// </summary>
    /// <param name="client">An SQS client object used to send the message.</param>
    /// <param name="queueName">A string representing the name of the queue
    /// to create.</param>
    /// <param name="kmsKeyId">The KmsKeyId used to encrypt the queue.</param>
    /// <returns>A CreateQueueResponse that contains information about the
    /// newly created queue.</returns>
    private static async Task<CreateQueueResponse> CreateQueue(IAmazonSQS client, string queueName, string kmsKeyId)
    {
      Console.WriteLine($"CreateQueue - queueName: [{queueName}]");
      int maxMessage = 256 * 1024;
      var attrs = new Dictionary<string, string>
      {
        // Common attributes
        // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-delay-queues.html
        // Delay FIFO queue are ok because all messages get delayed
        // FIFO queues don't allow per message delay
        {
          QueueAttributeName.DelaySeconds, TimeSpan.FromSeconds(0).TotalSeconds.ToString(CultureInfo.InvariantCulture) // Should not use
        },
        { QueueAttributeName.KmsMasterKeyId, kmsKeyId },
        { QueueAttributeName.MaximumMessageSize, maxMessage.ToString() },
        { QueueAttributeName.MessageRetentionPeriod, TimeSpan.FromDays(4).TotalSeconds.ToString(CultureInfo.InvariantCulture) },
        {
          QueueAttributeName.ReceiveMessageWaitTimeSeconds, // Long polling - max 20s
          TimeSpan.FromSeconds(20).TotalSeconds.ToString(CultureInfo.InvariantCulture)
        },
        {
          QueueAttributeName.VisibilityTimeout, // The default visibility timeout for a message is 30 seconds. The minimum is 0 seconds. The maximum is 12 hours.
          TimeSpan.FromSeconds(30).TotalSeconds.ToString(CultureInfo.InvariantCulture)
        },
      };

      var request = new CreateQueueRequest { Attributes = attrs, QueueName = queueName, };

      var response = await client.CreateQueueAsync(request);
      Console.WriteLine($"Created a queue with URL : {response.QueueUrl}");

      return response;
    }

    private static async Task<string> CreateTopic(string topicName, string kmsKeyId)
    {
      Console.WriteLine($"CreateTopic - topicName: [{topicName}]");
      var request = new CreateTopicRequest
      {
        Name = topicName,
        Attributes = new Dictionary<string, string>
        {
          { "KmsMasterKeyId", kmsKeyId },
        },
      };

      var response = await ClientSns.CreateTopicAsync(request);

      return response.TopicArn;
    }

    /// <summary>
    /// Display message information for a list of Amazon SQS messages.
    /// </summary>
    /// <param name="messages">The list of Amazon SQS Message objects to display.</param>
    private static void DisplayMessages(List<Message> messages)
    {
      Console.WriteLine($"DisplayMessages - messages.Count: [{messages.Count}]");
      messages.ForEach(m =>
      {
        Console.WriteLine($"For message ID {m.MessageId}:");
        Console.WriteLine($"  Body: {m.Body}");
        Console.WriteLine($"  Receipt handle: {m.ReceiptHandle}");
        Console.WriteLine($"  MD5 of body: {m.MD5OfBody}");
        Console.WriteLine($"  MD5 of message attributes: {m.MD5OfMessageAttributes}");
        Console.WriteLine("  Message Attributes:");

        foreach (var attr in m.MessageAttributes)
        {
          Console.WriteLine($"\t {attr.Key}: {attr.Value}");
        }

        Console.WriteLine("  Attributes:");
        foreach (var attr in m.Attributes)
        {
          Console.WriteLine($"\t {attr.Key}: {attr.Value}");
        }
      });
    }

    /// <summary>
    /// Retrieve the queue URL for the queue named in the queueName
    /// property using the client object.
    /// </summary>
    /// <param name="queueName">A string representing  name of the queue
    /// for which to retrieve the URL.</param>
    /// <returns>The URL of the queue.</returns>
    private static async Task<string> GetQueueUrl(string queueName)
    {
      var request = new GetQueueUrlRequest { QueueName = queueName, };

      GetQueueUrlResponse response = await ClientSqs.GetQueueUrlAsync(request);
      return response.QueueUrl;
    }

    private static AppSettings LoadAppsettings()
    {
      var builder = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", optional: false);

      IConfiguration config = builder.Build();

      var appSettings = config.Get<AppSettings>();
      Console.WriteLine($"FirstQueueName: [{appSettings.FirstQueueName}]");
      Console.WriteLine($"KmsKeyId: [{appSettings.KmsKeyId}]");
      Console.WriteLine($"SecondQueueName: [{appSettings.SecondQueueName}]");
      Console.WriteLine($"TopicName: [{appSettings.TopicName}]");
      return appSettings;
    }

    /// <summary>
    /// Publishes a message to an Amazon SNS topic.
    /// </summary>
    /// to the Amazon SNS topic.
    /// <param name="topicName">The name of the topic.</param>
    /// <param name="messageText">The text of the message.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    private static async Task PublishToTopicAsync(
      string topicName,
      string messageText)
    {
      Console.WriteLine($"PublishToTopicAsync - topicName: [{topicName}], messageText: [{messageText}]");
      var topic = await ClientSns.FindTopicAsync(topicName);

      var request = new PublishRequest { TopicArn = topic.TopicArn, Message = messageText, };

      var response = await ClientSns.PublishAsync(request);

      Console.WriteLine($"Successfully published message ID: {response.MessageId}");
    }

    /// <summary>
    /// Retrieves the message from the queue at the URL passed in the
    /// queueURL parameters using the client.
    /// </summary>
    /// <param name="queueName">The URL of the queue from which to retrieve
    /// a message.</param>
    /// <returns>The response from the call to ReceiveMessageAsync.</returns>
    private static async Task<ReceiveMessageResponse> ReceiveAndDeleteMessage(string queueName)
    {
      Console.WriteLine($"ReceiveAndDeleteMessage - queueName: [{queueName}]");
      var attributeNames = new List<string> { "All" };
      int maxNumberOfMessages = 5;
      var visibilityTimeout = 10;
      var waitTimeSeconds = 20; // Long polling - max 20s

      var queueUrl = await GetQueueUrl(queueName);

      // Receive a single message from the queue.
      var receiveMessageRequest = new ReceiveMessageRequest
      {
        AttributeNames = attributeNames,
        MaxNumberOfMessages = maxNumberOfMessages,
        MessageAttributeNames = { "All" },
        QueueUrl = queueUrl,
        VisibilityTimeout = visibilityTimeout,
        WaitTimeSeconds = waitTimeSeconds,
      };

      var receiveMessageResponse = await ClientSqs.ReceiveMessageAsync(receiveMessageRequest);

      if (receiveMessageResponse.Messages.Count > 0)
      {
        Console.WriteLine($"For Queue Name {queueName}");
        Console.WriteLine("---------------------------------------------");
        DisplayMessages(receiveMessageResponse.Messages);
      }

      // Delete the received message from the queue.
      foreach (var message in receiveMessageResponse.Messages)
      {
        var deleteMessageRequest = new DeleteMessageRequest { QueueUrl = queueUrl, ReceiptHandle = message.ReceiptHandle, };

        await ClientSqs.DeleteMessageAsync(deleteMessageRequest);
      }

      return receiveMessageResponse;
    }

    private static async Task<string> SubscribeQueueAsync(string queueName, string topicName)
    {
      Console.WriteLine($"SubscribeQueueAsync - queueName: [{queueName}], topicName: [{topicName}]");
      var queueUrlResponse = await ClientSqs.GetQueueUrlAsync(queueName); // TODO on peut aussi simplement passer l'URL en constante
      var topic = await ClientSns.FindTopicAsync(topicName);

      return await ClientSns.SubscribeQueueAsync(topic.TopicArn, ClientSqs, queueUrlResponse.QueueUrl);
    }
  }
}
