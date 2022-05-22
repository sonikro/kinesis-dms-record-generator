import moment from 'moment';
import { GenerateKinesisEventsInput, GenerateKinesisEventsOutput } from '.';
import File from '../../domain/File';
import { JSONObject } from '../../domain/JSONObject';
import { FileSystem } from '../../providers/FileSystem';
import { KinesisClient } from '../../providers/KinesisClient';
import { ProgressBar } from '../../providers/ProgressBar';
import { UseCase } from '../UseCase';

/**
 * This use case will load JSON files, and automatically simulate PutRecords on your Localstack Kinesis
 */
export class GenerateKinesisEvents
  implements UseCase<GenerateKinesisEventsInput, Promise<GenerateKinesisEventsOutput>>
{
  constructor(
    private readonly fileSystem: FileSystem,
    private readonly progressBar: ProgressBar,
    private readonly kinesisClient: KinesisClient,
  ) {}

  async invoke(input: GenerateKinesisEventsInput): Promise<GenerateKinesisEventsOutput> {
    const { recordFileDir, operation } = input;
    const filesToLoad = this.fileSystem.readDir(recordFileDir);
    const files = filesToLoad
      .map((filename) => {
        const content = this.fileSystem.readJsonFile(`${recordFileDir}/${filename}`);
        return new File(filename).getFileProperties(content);
      })
      .sort((a, b) => b.order - a.order);

    console.info(`Running on the following order: ${files.map((file) => file.filename)}`);
    const multiProgressBar = this.progressBar.getMultiBar();
    const totalProgressBar = this.progressBar.createSingleBar({
      total: filesToLoad.length,
      startValue: 0,
      payload: {
        fileName: 'TOTAL PROGRESS',
        dataType: 'Files',
      },
    });

    let loadedRecords = 0;
    for (const file of files) {
      const content = Array.isArray(file.content) ? file.content : [file.content];
      const payloads = content.map((record: JSONObject) =>
        this.generateKinesisPayload({
          record,
          schema: file.schema,
          table: file.table,
          operation,
        }),
      );
      const totalChunks = Math.ceil(payloads.length / input.chunkSize);
      const chunkProgressBar = this.progressBar.createSingleBar({
        total: totalChunks,
        startValue: 0,
        payload: {
          fileName: file.filename,
          dataType: 'Chunks',
        },
      });

      let sliceInitialIndex = 0;
      for (let i = 0; i < totalChunks; i++) {
        const payloadChunks = payloads.slice(
          sliceInitialIndex,
          sliceInitialIndex + input.chunkSize,
        );
        sliceInitialIndex += input.chunkSize;
        await this.kinesisClient.send(payloadChunks);
        loadedRecords += payloadChunks.length;
        chunkProgressBar.increment();
      }
      totalProgressBar.increment();
    }
    multiProgressBar.stop();

    return {
      ...input,
      loadedRecords,
    };
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

  static validateChunkSize(chunkSize: string): string {
    /**
     * Each `PutRecords` request can support up to 500 records.
     * https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
     */
    const CHUNK_MAX_SIZE = 500;
    if (+chunkSize >= 1 && +chunkSize <= CHUNK_MAX_SIZE) {
      return chunkSize;
    }
    const errorMessage = `Invalid chunk size ${chunkSize}. Please Make sure to select a number between 1 and ${CHUNK_MAX_SIZE}`;
    throw Error(errorMessage);
  }
}
