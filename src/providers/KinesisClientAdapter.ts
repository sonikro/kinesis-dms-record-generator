import { KinesisClient, PutRecordsCommand, PutRecordsRequestEntry } from '@aws-sdk/client-kinesis';
import { KinesisClient as KinesisClientInterface } from '../core/providers/KinesisClient';

export class KinesisClientAdapter implements KinesisClientInterface {
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
    const messages = this.createMessages(payloads);
    const command = this.createCommand(messages);
    await this.kinesisClient.send(command);
  }

  private createMessages(payloads: Record<string, any>): PutRecordsRequestEntry[] {
    return payloads.map((payload: any) => {
      return {
        Data: Buffer.from(JSON.stringify(payload)),
        PartitionKey: this.partitionKey,
      } as PutRecordsRequestEntry;
    });
  }

  private createCommand(messages: PutRecordsRequestEntry[]): PutRecordsCommand {
    return new PutRecordsCommand({
      Records: messages,
      StreamName: this.streamName,
    });
  }
}
