import { GenerateKinesisEvents, Operation } from '.';
import { mockDMSPayload, mockJsonPayloads, streamProps } from '../../../test/mocks';
import { FileSystem } from '../../providers/FileSystem';
import { KinesisClient } from '../../providers/KinesisClient';
import { ProgressBar } from '../../providers/ProgressBar';

describe('GenerateKinesisEvents', () => {
  const makeSut = () => {
    const mockDirContent = ['1.schema1.table1.json'];
    const singleBarIncrement = jest.fn();
    const progressBarFunctions = {
      create: jest.fn().mockReturnValue({
        increment: singleBarIncrement,
      }),
      stop: jest.fn(),
    };
    const progressBar: ProgressBar = {
      getMultiBar: jest.fn().mockReturnValue(progressBarFunctions),
      createSingleBar: jest.fn().mockReturnValue({
        increment: singleBarIncrement,
      }),
    };
    const fileSystem: FileSystem = {
      readJsonFile: jest.fn().mockReturnValue(mockJsonPayloads),
      readDir: jest.fn().mockReturnValue(mockDirContent),
    };
    const kinesisClient: KinesisClient = {
      send: jest.fn(),
    };
    const sut = new GenerateKinesisEvents(fileSystem, progressBar, kinesisClient);
    return {
      sut,
      fileSystem,
      progressBar,
      kinesisClient,
      mockJsonContent: mockJsonPayloads,
      mockDirContent,
      singleBarIncrement,
    };
  };

  it('correctly loads file and invoke KinesisClient to put-records on stream with chunk size bigger than 1 (chunkSize = 10)', async () => {
    // Given
    const { sut, fileSystem, mockDirContent, progressBar, kinesisClient, singleBarIncrement } =
      makeSut();
    const now = new Date('2022-05-22T10:00:00');
    Date.now = jest.fn().mockReturnValue(now);
    const streamProperties = {
      ...streamProps,
      filename: mockDirContent[0],
      chunkSize: '10',
    };

    // When
    await sut.invoke({
      partitionKey: streamProperties.partitionKey,
      operation: 'load' as Operation,
      streamName: streamProperties.streamName,
      recordFileDir: streamProperties.recordFileDir,
      localstackEndpoint: streamProperties.endpoint,
      chunkSize: +streamProperties.chunkSize,
    });
    // Then
    expect(fileSystem.readDir).toHaveBeenCalledWith(streamProperties.recordFileDir);
    expect(progressBar.getMultiBar).toHaveBeenCalled();
    expect(fileSystem.readJsonFile).toHaveBeenCalledWith(
      `${streamProperties.recordFileDir}/${streamProperties.filename}`,
    );
    expect(kinesisClient.send).toHaveBeenCalledWith(mockDMSPayload);
    expect(singleBarIncrement).toHaveBeenCalled();
  });
  it('correctly loads file and invoke KinesisClient to put-records on stream with chunk size 1', async () => {
    // Given
    const { sut, mockDirContent, kinesisClient } = makeSut();
    const now = new Date('2022-05-22T10:00:00');
    Date.now = jest.fn().mockReturnValue(now);
    const streamProperties = {
      ...streamProps,
      filename: mockDirContent[0],
      chunkSize: '1',
    };

    // When
    await sut.invoke({
      partitionKey: streamProperties.partitionKey,
      operation: 'load' as Operation,
      streamName: streamProperties.streamName,
      recordFileDir: streamProperties.recordFileDir,
      localstackEndpoint: streamProperties.endpoint,
      chunkSize: +streamProperties.chunkSize,
    });
    // Then
    const [firstChunk, secondChunk] = mockDMSPayload;
    expect(kinesisClient.send).toHaveBeenNthCalledWith(1, [firstChunk]);
    expect(kinesisClient.send).toHaveBeenNthCalledWith(2, [secondChunk]);
  });

  it('correctly loads file and invoke KinesisClient to put-records on stream with chunk size 1 using a single json', async () => {
    // Given
    const { sut, mockDirContent, kinesisClient, fileSystem } = makeSut();
    const now = new Date('2022-05-22T10:00:00');
    const [firstJson] = mockJsonPayloads;
    fileSystem.readJsonFile = jest.fn().mockReturnValue(firstJson);
    Date.now = jest.fn().mockReturnValue(now);
    const streamProperties = {
      ...streamProps,
      filename: mockDirContent[0],
      chunkSize: '1',
    };

    // When
    await sut.invoke({
      partitionKey: streamProperties.partitionKey,
      operation: 'load' as Operation,
      streamName: streamProperties.streamName,
      recordFileDir: streamProperties.recordFileDir,
      localstackEndpoint: streamProperties.endpoint,
      chunkSize: +streamProperties.chunkSize,
    });
    // Then
    const [firstChunk] = mockDMSPayload;
    expect(kinesisClient.send).toHaveBeenNthCalledWith(1, [firstChunk]);
  });

  it('throws error if filename is incorrect', async () => {
    // Given
    const { sut, fileSystem } = makeSut();
    const expected = {
      partitionKey: '1',
      operation: 'load',
      streamName: 'stream-name',
      recordFileDir: 'fileDir',
      localstackEndpoint: 'http://localhost:4566',
      chunkSize: '1',
      filename: 'wrong.format',
    };
    fileSystem.readDir = jest.fn().mockReturnValue([expected.filename]);
    // When
    const act = async () => {
      await sut.invoke({
        partitionKey: expected.partitionKey,
        operation: expected.operation as Operation,
        streamName: expected.streamName,
        recordFileDir: expected.recordFileDir,
        localstackEndpoint: expected.localstackEndpoint,
        chunkSize: +expected.chunkSize,
      });
    };
    // Then
    await expect(act).rejects.toThrowError(
      `Invalid file name ${expected.filename}. Files should follow the pattern order.schema.table.json`,
    );
  });

  it('should load files from bigger order to lower order', async () => {
    // Given
    const { sut, fileSystem } = makeSut();
    const now = new Date();
    Date.now = jest.fn().mockReturnValue(now);

    const mockDirContent = [
      '1.schema1.table1.json',
      '3.schema3.table3.json',
      '4.schema4.table4.json',
      '2.schema2.table2.json',
    ];

    fileSystem.readDir = jest.fn().mockReturnValue(mockDirContent);

    const eventDefinition = {
      partitionKey: '1',
      operation: 'load',
      streamName: 'stream-name',
      recordFileDir: 'fileDir',
      localstackEndpoint: 'http://localhost:4566',
      chunkSize: '1',
      filename: mockDirContent,
    };

    // When
    await sut.invoke({
      partitionKey: eventDefinition.partitionKey,
      operation: eventDefinition.operation as Operation,
      streamName: eventDefinition.streamName,
      recordFileDir: eventDefinition.recordFileDir,
      localstackEndpoint: eventDefinition.localstackEndpoint,
      chunkSize: +eventDefinition.chunkSize,
    });
  });

  it('should return the operation if is valid', () => {
    const expectedOperation = 'load';
    const operation = GenerateKinesisEvents.validateOperation('load');
    expect(operation).toBe(expectedOperation);
  });

  it('should throw an error if operation is not valid', () => {
    const invalidOperation = 'banana';
    const expectedErrorMessage = `Invalid operation ${invalidOperation}. Please Make sure to select one of the following: [LOAD, INSERT, UPDATE, DELETE]`;
    const act = () => GenerateKinesisEvents.validateOperation(invalidOperation);
    expect(act).toThrow(expectedErrorMessage);
  });

  it('should return the chunkSize if is valid', () => {
    const expectedChunkSize = '1';
    const chunkSize = GenerateKinesisEvents.validateChunkSize('1');
    expect(chunkSize).toBe(expectedChunkSize);
  });

  it('should throw an error if chunkSize is not valid', () => {
    const invalidChunkSize = '0';
    const expectedErrorMessage = `Invalid chunk size ${invalidChunkSize}. Please Make sure to select a number between 1 and 500`;
    const act = () => GenerateKinesisEvents.validateChunkSize(invalidChunkSize);
    expect(act).toThrow(expectedErrorMessage);
  });
});
