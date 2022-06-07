#!/usr/bin/env node

import { Command } from 'commander';
import { GenerateKinesisEvents } from '../core/usecase/GenerateKinesisEvents';
import { KinesisClientAdapter } from '../providers/KinesisClientAdapter';
import { NodeFileSystem } from '../providers/NodeFileSystem';
import { NodeProgressBar } from '../providers/NodeProgressBar';

(async () => {
  const program = new Command();
  program
    .requiredOption('-d, --directory <value>', 'directory where the JSON files are located')
    .requiredOption('-s, --stream-name <value>', 'local-stack kinesis stream name')
    .option('-p, --partition-key <value>', 'local-stack kinesis partition key', '1')
    .option('-e, --localstack-endpoint <value>', 'local-stack endpoint', 'http://localhost:4566')
    .option(
      '-o, --operation <value>',
      'operation name you want to simulate',
      GenerateKinesisEvents.validateOperation,
      'load',
    )
    .option(
      '-b, --batch-size <value>',
      'batch-size for batch processing (1 to 500)',
      GenerateKinesisEvents.validateBatchSize,
      '1',
    )
    .parse();

  const response = program.opts();
  const useCase = new GenerateKinesisEvents(
    new NodeFileSystem(),
    new NodeProgressBar(),
    new KinesisClientAdapter(
      response.localstackEndpoint,
      response.streamName,
      response.partitionKey,
    ),
  );

  try {
    const usecaseResponse = await useCase.invoke({
      recordFileDir: response.directory,
      streamName: response.streamName,
      partitionKey: response.partitionKey,
      localstackEndpoint: response.localstackEndpoint,
      operation: response.operation,
      batchSize: response.batchSize,
    });
    console.info(usecaseResponse);
  } catch (err: any) {
    console.error('Error running cli', {
      error: err.message,
    });
    program.outputHelp();
    process.exit(1);
  }
})();
