#!/usr/bin/env node
import prompts from "prompts"
import {GenerateKinesisEvents} from "../core/usecase/generateKinesisEvents";
import {NodeFileSystem} from "../providers/NodeFileSystem";
import {NodeShell} from "../providers/NodeShell";

(async () => {
    const response = await prompts([
        {
            type: 'text',
            name: 'recordFileDir',
            message: `What's the folder where the JSON files are located ?`,
        },
        {
            type: 'text',
            name: 'streamName',
            message: `What's the name of the kinesis stream?`,
        },
        {
            type: 'text',
            name: 'partitionKey',
            message: `What's the partition key?`,
            initial: "1"
        },
        {
            type: 'text',
            name: 'localstackEndpoint',
            message: `What's the localstack endpoint?`,
            initial: "http://localhost:4566"
        },
        {
            type: 'select',
            name: 'operation',
            message: `What's the name operation you want to simulate?`,
            choices: [
                { title: "LOAD", value: "load"},
                { title: "INSERT", value: "insert"},
                { title: "UPDATE", value: "update"},
                { title: "DELETE", value: "delete"}
            ]
        }
    ]);


    const useCase = new GenerateKinesisEvents(new NodeFileSystem(), new NodeShell())
    await useCase.invoke({
        streamName: response.streamName,
        operation: response.operation,
        recordFileDir: response.recordFileDir,
        partitionKey: response.partitionKey,
        localstackEndpoint: response.localstackEndpoint
    })
    console.log(response);
})()