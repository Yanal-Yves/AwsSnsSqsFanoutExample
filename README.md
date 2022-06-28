# Introduction 
Example that shows the AWS SNS SQS fanout pattern by using the AWS SDK for .NET and .NET Core 3.1.
This console app :
1) Creates an AWS Simple Notification (SNS) Topic
2) Creates two AWS Simple Queue Service (SQS) queues
3) Subscribe the two queues to the topic
4) Publish messages to the topic
5) Receive the messages from the two queues
