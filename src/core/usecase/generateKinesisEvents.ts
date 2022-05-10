import moment from 'moment';
import { UseCase } from './UseCase';
import { FileSystem } from '../providers/FileSystem';
import { Shell } from '../providers/Shell';
import { JSONObject } from '../domain/JSONObject';

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
  chunkSize: number;
}

/**
 * This use case will load JSON files, and automatically simulate PutRecords on your Localstack Kinesis
 */
export class GenerateKinesisEvents
  implements UseCase<GenerateKinesisEventsInput, Promise<void>>
{
  constructor(
    private readonly fileSystem: FileSystem,
    private readonly shell: Shell,
  ) {}

  async invoke(input: GenerateKinesisEventsInput): Promise<void> {
    const {
      recordFileDir,
      streamName,
      partitionKey,
      operation,
      localstackEndpoint,
      chunkSize,
    } = input;
    const filesToLoad = this.fileSystem.readDir(recordFileDir);

    const loadedFiles = filesToLoad
      .map((filepath) => {
        const fileParts = filepath.split('.');
        if (fileParts.length < 4) {
          throw new Error(
            `Invalid file name ${filepath}. Files should follow the pattern order.schema.table.json`,
          );
        }
        return {
          content: this.fileSystem.readJsonFile(`${recordFileDir}/${filepath}`),
          order: parseInt(fileParts[0], 10),
          schema: fileParts[1],
          table: fileParts[2],
        };
      })
      .sort((a, b) => b.order - a.order);

    console.log(
      `Running on the following order:\n${loadedFiles
        .map((entry) => `${entry.order}-${entry.schema}-${entry.table}`)
        .join('\n')}`,
    );

    for (const loadedFile of loadedFiles) {
      const content = Array.isArray(loadedFile.content)
        ? loadedFile.content
        : [loadedFile.content];

      const payloadList = content.map((record: JSONObject) =>
        GenerateKinesisEvents.generateKinesisPayload({
          record,
          schema: loadedFile.schema,
          table: loadedFile.table,
          operation,
        }),
      );

      const instructions = GenerateKinesisEvents.generateCommandLine({
        payloadList,
        streamName,
        partitionKey,
        endpoint: localstackEndpoint,
        chunkSize: +chunkSize,
      });

      for (const command of instructions) {
        const stdout = await this.shell.execute(command);
        console.log(stdout);
      }
    }
  }

  private static generateCommandLine(params: {
    payloadList: string[];
    streamName: string;
    partitionKey: string;
    endpoint: string;
    chunkSize: number;
  }): string[] {
    const commandList = [];

    for (let i = 0; i < params.payloadList.length; i += params.chunkSize) {
      const chunk = params.payloadList.slice(i, i + params.chunkSize);
      let command = `aws --endpoint-url=${params.endpoint} kinesis put-records --stream-name ${params.streamName} --records `;
      for (const payload of chunk) {
        command += `Data=${payload},PartitionKey=${params.partitionKey} `;
      }
      commandList.push(command);
    }

    return commandList;
  }

  private static generateKinesisPayload(params: {
    schema: string;
    table: string;
    operation: string;
    record: JSONObject;
  }): string {
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
    return `${Buffer.from(JSON.stringify(payload)).toString('base64')}`;
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
