#!/usr/bin/env node

import { Command } from 'commander';
import { GenerateKinesisEvents } from '../core/usecase/GenerateKinesisEvents';
import { KinesisAdapter } from '../providers/KinesisAdapter';
import { NodeFileSystem } from '../providers/NodeFileSystem';
import { NodeProgressBar } from '../providers/NodeProgressBar';

(async () => {
  const program = new Command();
  program
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
    .parse();

  const response = program.opts();
  const useCase = new GenerateKinesisEvents(
    new NodeFileSystem(),
    new NodeProgressBar(),
    new KinesisAdapter(
      response.localstackEndpoint,
      response.streamName,
      response.partitionKey,
    ),
  );

  try {
    await useCase.invoke({
      recordFileDir: response.directory,
      streamName: response.streamName,
      partitionKey: response.partitionKey,
      localstackEndpoint: response.localstackEndpoint,
      operation: response.operation,
    });
    console.info(response);
  } catch (err: any) {
    console.error('Error running cli', {
      error: err.message,
    });
    program.outputHelp();
    process.exit(1);
  }
})();
