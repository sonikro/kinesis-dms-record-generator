import {GenerateKinesisEvents, Operation} from "./generateKinesisEvents";
import {FileSystem} from "../providers/FileSystem";
import {Shell} from "../providers/Shell";
import {JSONObject} from "../domain/JSONObject";
import moment from "moment";

describe("generatKinesisEvents", () => {
    const makeSut = () => {
        const mockDirContent = ["schema1.table1.json"]
        const mockJsonContent: JSONObject[] = [
            {
                ID: 1,
                NAME: "Joselito"
            }
        ]
        const fileSystem: FileSystem = {
            readJsonFile: jest.fn().mockReturnValue(mockJsonContent),
            readDir: jest.fn().mockReturnValue(mockDirContent)
        }

        const shell: Shell = {
            execute: jest.fn()
        }
        const sut = new GenerateKinesisEvents(fileSystem, shell)
        return {
            sut,
            fileSystem,
            shell,
            mockJsonContent,
            mockDirContent
        }
    }

    it("correctly loads file and invoke AWS CLI to put-record on stream", async () => {
        //Given
        const {sut, fileSystem, shell, mockJsonContent, mockDirContent} = makeSut()
        const now = new Date()
        Date.now = jest.fn().mockReturnValue(now)
        const expected = {
            partitionKey: "1",
            operation: "load",
            streamName: "stream-name",
            recordFileDir: "fileDir",
            localstackEndpoint: "http://localhost:4566",
            filename: mockDirContent[0],
            recordContent: {
                "data": mockJsonContent[0],
                "metadata": {
                    "timestamp": moment(now).format("yyyy-MM-DDTHH:mm:ss.SSSS[Z]"),
                    "record-type": "data",
                    "operation": "load",
                    "partition-key-type": "primary-key",
                    "schema-name": "schema1",
                    "table-name": "table1"
                }
            }
        }
        //When
        await sut.invoke({
            partitionKey: expected.partitionKey,
            operation: expected.operation as Operation,
            streamName: expected.streamName,
            recordFileDir: expected.recordFileDir,
            localstackEndpoint: expected.localstackEndpoint
        })
        //Then
        expect(fileSystem.readDir).toHaveBeenCalledWith(expected.recordFileDir)
        expect(fileSystem.readJsonFile).toHaveBeenCalledWith(`${expected.recordFileDir}/${expected.filename}`)
        expect(shell.execute).toHaveBeenCalledWith(`aws --endpoint-url=${expected.localstackEndpoint} kinesis put-record --stream-name ${expected.streamName} --partition-key ${expected.partitionKey} --data ${Buffer.from(JSON.stringify(expected.recordContent)).toString("base64")}`)

    })

    it("throws error if filename is incorrect", async () => {
        //Given
        const {sut, fileSystem, shell, mockJsonContent, mockDirContent} = makeSut()
        const expected = {
            partitionKey: "1",
            operation: "load",
            streamName: "stream-name",
            recordFileDir: "fileDir",
            localstackEndpoint: "http://localhost:4566",
            filename: "wrong.format",
        }
        fileSystem.readDir = jest.fn().mockReturnValue([expected.filename])
        //When
        const act = async () => {
            await sut.invoke({
                partitionKey: expected.partitionKey,
                operation: expected.operation as Operation,
                streamName: expected.streamName,
                recordFileDir: expected.recordFileDir,
                localstackEndpoint: expected.localstackEndpoint
            })
        }
        //Then
        await expect(act).rejects.toThrowError(`Invalid file name ${expected.filename}. Files should follow the pattern schema.table.json`)

    })
})