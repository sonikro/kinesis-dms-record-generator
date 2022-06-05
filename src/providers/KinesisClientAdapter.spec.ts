import * as Kinesis from '@aws-sdk/client-kinesis';
import { PutRecordsCommandInput } from '@aws-sdk/client-kinesis';
import { mockJsonPayloads, mockPutRecordsRequestEntry, streamProps } from '../test/mocks';
import { KinesisClientAdapter } from './KinesisClientAdapter';

const mockSend = jest.fn();
jest.mock('@aws-sdk/client-kinesis', () => ({
  KinesisClient: jest.fn().mockImplementation(() => ({
    send: mockSend,
  })),
  PutRecordsCommand: jest.fn().mockImplementation(() => ({
    Records: mockPutRecordsRequestEntry,
    StreamName: streamProps.streamName,
  })),
}));

describe('KinesisClientAdapter', () => {
  it('it should send put records command to kinesis client', async () => {
    // Given
    const kinesisClientAdapter = new KinesisClientAdapter(
      streamProps.endpoint,
      streamProps.streamName,
      streamProps.partitionKey,
    );
    const expectedPutRecordsCommand: PutRecordsCommandInput = {
      Records: mockPutRecordsRequestEntry,
      StreamName: streamProps.streamName,
    };
    const putRecordsCommandSpy = jest.spyOn(Kinesis, 'PutRecordsCommand');
    // When
    await kinesisClientAdapter.send(mockJsonPayloads);
    // Then
    expect(putRecordsCommandSpy).toHaveBeenCalledWith(expectedPutRecordsCommand);
    expect(mockSend).toHaveBeenCalledWith(expectedPutRecordsCommand);
  });
});
