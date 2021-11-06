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
      'Directory where the JSON files are located',
    )
    .requiredOption(
      '-s, --stream-name <value>',
      'Name of the local-stack kinesis stream',
    )
    .option(
      '-p, --partition-key <value>',
      'Local-stack kinesis partition key',
      '1',
    )
    .option(
      '-e, --localstack-endpoint <value>',
      'Local-stack endpoint',
      'http://localhost:4566',
    )
    .option(
      '-o, --operation <value>',
      'Operation name you want to simulate?',
      GenerateKinesisEvents.validateOperation,
      'load',
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
  });

  console.log(response);
})();
