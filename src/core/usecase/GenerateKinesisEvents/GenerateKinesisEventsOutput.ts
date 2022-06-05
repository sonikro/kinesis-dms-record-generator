import { GenerateKinesisEventsInput } from '.';

export interface GenerateKinesisEventsOutput extends GenerateKinesisEventsInput {
  /**
   * Total of loaded JSON to kinesis stream
   */
  loadedRecords: number;
}
