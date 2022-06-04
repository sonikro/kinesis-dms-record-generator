export type Operation = 'load' | 'insert' | 'update' | 'delete';

export interface GenerateKinesisEventsInput {
  /**
   * The name of the kinesis stream running in your localstack
   */
  streamName: string;
  /**
   * The partition key to PutRecords
   */
  partitionKey: string;
  /**
   * The path where all the JSON files are located
   */
  recordFileDir: string;
  /**
   * The type of operation you want to simulate
   */
  operation: Operation;
  /**
   * Localstack Endpoint
   */
  localstackEndpoint: string;
  /**
   * Flag to indicate you want to process as batch
   */
  batchSize: number;
}
