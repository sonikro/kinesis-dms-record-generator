import { PutRecordsRequestEntry } from '@aws-sdk/client-kinesis';
import { JSONObject } from '../core/domain/JSONObject';

export const streamProps = {
  partitionKey: 'any-partition-key',
  streamName: 'any-stream-name',
  endpoint: 'http://any-enpoint:4566',
  operation: 'load',
  recordFileDir: 'fileDir',
  chunkSize: '10',
};
export const firstJsonPayload: JSONObject = {
  id: 1,
  name: 'Joselito Naruto',
  age: 18,
};
export const secondJsonPayload: JSONObject = {
  id: 1,
  name: 'Nagas Bike',
  age: 25,
};
export const mockJsonPayloads = [firstJsonPayload, secondJsonPayload];
export const mockPutRecordsRequestEntry: PutRecordsRequestEntry[] = [
  {
    Data: Buffer.from(JSON.stringify(firstJsonPayload)),
    PartitionKey: streamProps.partitionKey,
  },
  {
    Data: Buffer.from(JSON.stringify(secondJsonPayload)),
    PartitionKey: streamProps.partitionKey,
  },
];

export const mockDMSPayload = [
  {
    data: { age: 18, id: 1, name: 'Joselito Naruto' },
    metadata: {
      operation: 'load',
      'partition-key-type': 'primary-key',
      'record-type': 'data',
      'schema-name': 'schema1',
      'table-name': 'table1',
      timestamp: '2022-05-22T10:00:00.0000Z',
    },
  },
  {
    data: { age: 25, id: 1, name: 'Nagas Bike' },
    metadata: {
      operation: 'load',
      'partition-key-type': 'primary-key',
      'record-type': 'data',
      'schema-name': 'schema1',
      'table-name': 'table1',
      timestamp: '2022-05-22T10:00:00.0000Z',
    },
  },
];

export const mockDirContent = [
  '1.schema1.table1.json',
  '3.schema3.table3.json',
  '4.schema4.table4.json',
  '2.schema2.table2.json',
];
