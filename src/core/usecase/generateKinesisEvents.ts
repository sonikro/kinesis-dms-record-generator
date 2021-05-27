import {UseCase} from "./UseCase";
import {FileSystem} from "../providers/FileSystem";
import {Shell} from "../providers/Shell";
import {JSONObject} from "../domain/JSONObject";
import moment from "moment";

export type Operation = "load" | "insert" | "update" | "delete"
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
    recordFileDir: string
    /**
     * The type of operation you want to simulate
     */
    operation: Operation
    /**
     * Localstack Endpoint
     */
    localstackEndpoint: string
}

/**
 * This use case will load JSON files, and automatically simulate PutRecords on your Localstack Kinesis
 */
export class GenerateKinesisEvents implements UseCase<GenerateKinesisEventsInput, Promise<void>> {
    constructor(
        private readonly fileSystem: FileSystem,
        private readonly shell: Shell
    ) {
    }

    async invoke(input: GenerateKinesisEventsInput): Promise<void> {
        const {recordFileDir, streamName, partitionKey, operation, localstackEndpoint} = input;
        const filesToLoad = this.fileSystem.readDir(recordFileDir)

        const loadedFiles = filesToLoad.map(filepath => {
            const fileParts = filepath.split(".")
            if (fileParts.length < 4) {
                throw new Error(`Invalid file name ${filepath}. Files should follow the pattern schema.table.json`)
            }
            return {
                content: this.fileSystem.readJsonFile(`${recordFileDir}/${filepath}`),
                order: parseInt(fileParts[0]),
                schema: fileParts[1],
                table: fileParts[2],
            }
        }).sort((a, b) => a.order - b.order)

        console.log(`Running on the following order:\n ${loadedFiles.map(entry => `${entry.order}-${entry.schema}-${entry.table}`).join("\n")}`)
        for (const loadedFile of loadedFiles) {
            const content = Array.isArray(loadedFile.content) ? loadedFile.content : [loadedFile.content]
            const instructions = content.map(
                (record: JSONObject) => GenerateKinesisEvents.generateCommandLine({
                        partitionKey,
                        record,
                        schema: loadedFile.schema,
                        table: loadedFile.table,
                        operation,
                        streamName,
                        endpoint: localstackEndpoint
                    }
                )
            )

            for (const command of instructions) {
                const stdout = await this.shell.execute(command)
                console.log(stdout)
            }
        }
    }

    private static generateCommandLine(params: {
        schema: string,
        table: string,
        operation: string,
        record: JSONObject,
        streamName: string,
        partitionKey: string,
        endpoint: string
    }) {
        const date = Date.now()
        const payload = {
            "data": params.record,
            "metadata": {
                "timestamp": moment(date).format("yyyy-MM-DDTHH:mm:ss.SSSS[Z]"),
                "record-type": "data",
                "operation": params.operation,
                "partition-key-type": "primary-key",
                "schema-name": params.schema,
                "table-name": params.table
            }
        }
        return `aws --endpoint-url=${params.endpoint} kinesis put-record --stream-name ${params.streamName} --partition-key ${params.partitionKey} --data ${Buffer.from(JSON.stringify(payload)).toString("base64")}`
    }
}