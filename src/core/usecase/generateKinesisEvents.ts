import cliProgress from 'cli-progress';
import moment from 'moment';
import { JSONObject } from '../domain/JSONObject';
import { FileSystem } from '../providers/FileSystem';
import { ProgressBar } from '../providers/ProgressBar';
import { StreamClient } from '../providers/StreamClient';
import { UseCase } from './UseCase';

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
}

/**
 * This use case will load JSON files, and automatically simulate PutRecords on your Localstack Kinesis
 */
export class GenerateKinesisEvents
  implements UseCase<GenerateKinesisEventsInput, Promise<void>>
{
  constructor(
    private readonly fileSystem: FileSystem,
    private readonly progressBar: ProgressBar,
    private readonly streamClient: StreamClient,
  ) {}

  async invoke(input: GenerateKinesisEventsInput): Promise<void> {
    const { recordFileDir, operation } = input;
    const filesToLoad = this.fileSystem.readDir(recordFileDir);

    const loadedFiles = filesToLoad
      .map((filepath) => {
        const fileParts = filepath.split('.');
        if (fileParts.length < 4) {
          throw new Error(
            `Invalid file name ${filepath}. Files should follow the pattern order.schema.table.json`,
          );
        }
        const [order, schema, table] = fileParts;
        return {
          content: this.fileSystem.readJsonFile(`${recordFileDir}/${filepath}`),
          order: parseInt(order, 10),
          schema,
          table,
        };
      })
      .sort((a, b) => b.order - a.order);

    console.info(
      'Running on the following order:',
      loadedFiles.map(
        (entry) => `${entry.order}-${entry.schema}-${entry.table}`,
      ),
    );

    const multiProgressBars = this.progressBar.createMultiBar({
      format:
        '{fileName} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total} {dataType}',
      preset: cliProgress.Presets.shades_classic,
    });
    const totalProgress = multiProgressBars.create(filesToLoad.length, 0, {
      fileName: 'TOTAL PROGRESS',
      dataType: 'Files',
    });
    for (const loadedFile of loadedFiles) {
      const content = Array.isArray(loadedFile.content)
        ? loadedFile.content
        : [loadedFile.content];

      const payloadList = content.map((record: JSONObject) =>
        this.generateKinesisPayload({
          record,
          schema: loadedFile.schema,
          table: loadedFile.table,
          operation,
        }),
      );

      await this.streamClient.send(payloadList);
      totalProgress.increment();
    }
    multiProgressBars.stop();
  }

  private generateKinesisPayload(params: {
    schema: string;
    table: string;
    operation: string;
    record: JSONObject;
  }): Record<string, any> {
    const date = Date.now();
    const payload = {
      data: params.record,
      metadata: {
        timestamp: moment(date).format('yyyy-MM-DDTHH:mm:ss.SSSS[Z]'),
        'record-type': 'data',
        operation: params.operation,
        'partition-key-type': 'primary-key',
        'schema-name': params.schema,
        'table-name': params.table,
      },
    };
    return payload;
  }

  static validateOperation(operation: string): string {
    const validOperations = ['LOAD', 'INSERT', 'UPDATE', 'DELETE'];

    if (validOperations.includes(operation.toUpperCase())) {
      return operation;
    }

    const validOperationsStr = validOperations.join(', ');
    const errorMessage = `Invalid operation ${operation}. Please Make sure to select one of the following: [${validOperationsStr}]`;
    throw Error(errorMessage);
  }
}
