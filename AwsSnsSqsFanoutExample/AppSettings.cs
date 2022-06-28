namespace AwsSnsSqsFanoutExample
{
  public class AppSettings
  {
    public string FirstQueueName { get; set; } = "prothesis-First-Test-Queue";

    public string KmsKeyId { get; set; } = string.Empty;

    public string SecondQueueName { get; set; } = "prothesis-Second-Test-Queue";

    public string TopicName { get; set; } = "prothesis-TopicTest";
  }
}
