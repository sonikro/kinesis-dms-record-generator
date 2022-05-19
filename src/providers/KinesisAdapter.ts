import {
  KinesisClient,
  PutRecordsCommand,
  PutRecordsRequestEntry,
} from '@aws-sdk/client-kinesis';
import { StreamClient } from '../core/providers/StreamClient';

export class KinesisAdapter implements StreamClient {
  private readonly region = 'sa-east-1';

  private readonly kinesisClient: KinesisClient;

  constructor(
    private readonly endpoint: string,
    private readonly streamName: string,
    private readonly partitionKey: string,
  ) {
    this.kinesisClient = new KinesisClient({
      region: this.region,
      endpoint: this.endpoint,
    });
  }

  async send(payloads: Record<string, any>[]): Promise<void> {
    const messages = payloads.map((payload) => {
      return {
        Data: Buffer.from(JSON.stringify(payload)),
        PartitionKey: this.partitionKey,
      } as PutRecordsRequestEntry;
    });
    const command = new PutRecordsCommand({
      Records: messages,
      StreamName: this.streamName,
    });
    await this.kinesisClient.send(command);
  }
}
