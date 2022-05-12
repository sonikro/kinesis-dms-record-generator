import moment from 'moment';
import cliProgress from 'cli-progress';
import { UseCase } from './UseCase';
import { FileSystem } from '../providers/FileSystem';
import { Shell } from '../providers/Shell';
import { JSONObject } from '../domain/JSONObject';
import { ProgressBar } from '../providers/ProgressBar';

const config = {
  CHUNK_CHARACTER_LIMIT: 120000,
  AWS_PUT_RECORDS_LIMIT: 500,
};

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
    private readonly progressBar: ProgressBar,
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
        .map((entry) => `\t${entry.order}-${entry.schema}-${entry.table}`)
        .join('\n')}`,
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

      const chunkProgressBar = multiProgressBars.create(
        instructions.length,
        0,
        {
          fileName: `${loadedFile.order}.${loadedFile.schema}.${loadedFile.table}`,
          dataType: 'Chunks',
        },
      );
      for (const command of instructions) {
        await this.shell.execute(command);
        chunkProgressBar.increment();
      }
      totalProgress.increment();
    }
    multiProgressBars.stop();
  }

  private static generateCommandLine(params: {
    payloadList: Array<string>;
    streamName: string;
    partitionKey: string;
    endpoint: string;
    chunkSize: number;
  }): Array<string> {
    const finalChunkSize: number = GenerateKinesisEvents.validateBatchSize({
      records: params.payloadList,
      chunkSize: params.chunkSize,
    });
    const commandList = [];
    for (let i = 0; i < params.payloadList.length; i += finalChunkSize) {
      const chunk = params.payloadList.slice(i, i + finalChunkSize);
      let command = `aws --endpoint-url=${params.endpoint} kinesis put-records --stream-name ${params.streamName} --records `;
      for (const payload of chunk) {
        command += `Data=${payload},PartitionKey=${params.partitionKey} `;
      }
      commandList.push(command);
    }
    return commandList;
  }

  private static validateBatchSize(params: {
    records: Array<string>;
    chunkSize: number;
  }): number {
    let totalPayloadsSize = 0;
    for (const payload of params.records) {
      totalPayloadsSize += payload.length;
    }
    const suggestedChunkSize = +(
      params.records.length /
      (totalPayloadsSize / config.CHUNK_CHARACTER_LIMIT)
    ).toFixed();
    if (params.chunkSize > suggestedChunkSize) {
      console.warn(
        `WARNING: The current chunk size ${params.chunkSize} was reduced to ${suggestedChunkSize} to avoid exceeding shell character limit`,
      );
      return suggestedChunkSize;
    }
    return params.chunkSize;
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

  static validateChunkSize(chunkSize: string): string {
    if (+chunkSize >= 1 && +chunkSize <= config.AWS_PUT_RECORDS_LIMIT) {
      return chunkSize;
    }
    const errorMessage = `Invalid chunk size ${chunkSize}. Please Make sure to select a number between 1 and ${config.AWS_PUT_RECORDS_LIMIT}`;
    throw Error(errorMessage);
  }
}
