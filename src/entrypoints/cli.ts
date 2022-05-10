#!/usr/bin/env node

import { Command } from 'commander';
import { GenerateKinesisEvents } from '../core/usecase/generateKinesisEvents';
import { NodeFileSystem } from '../providers/NodeFileSystem';
import { NodeShell } from '../providers/NodeShell';

(async () => {
  const command = new Command();
  command
    .requiredOption(
      '-d, --directory <value>',
      'directory where the JSON files are located',
    )
    .requiredOption(
      '-s, --stream-name <value>',
      'local-stack kinesis stream name',
    )
    .option(
      '-p, --partition-key <value>',
      'local-stack kinesis partition key',
      '1',
    )
    .option(
      '-e, --localstack-endpoint <value>',
      'local-stack endpoint',
      'http://localhost:4566',
    )
    .option(
      '-o, --operation <value>',
      'operation name you want to simulate',
      GenerateKinesisEvents.validateOperation,
      'load',
    )
    .option(
      '-c, --chunk-size <value>',
      'chunk-size for batch processing (1 to 500)',
      GenerateKinesisEvents.validateChunkSize,
      '1',
    )
    .parse();

  const response = command.opts();

  const useCase = new GenerateKinesisEvents(
    new NodeFileSystem(),
    new NodeShell(),
  );

  await useCase.invoke({
    recordFileDir: response.directory,
    streamName: response.streamName,
    partitionKey: response.partitionKey,
    localstackEndpoint: response.localstackEndpoint,
    operation: response.operation,
    chunkSize: response.chunkSize,
  });

  console.log(response);
})();
