import {JSONObject} from "../domain/JSONObject";

export interface FileSystem {
    readDir: (dirname: string) => string[]
    readJsonFile: (filepath: string) => JSONObject[] | JSONObject
}