import { readdirSync, readFileSync } from 'fs';
import { JSONObject } from '../core/domain/JSONObject';
import { FileSystem } from '../core/providers/FileSystem';

export class NodeFileSystem implements FileSystem {
  readDir(dirname: string): string[] {
    return readdirSync(dirname);
  }

  readJsonFile(filepath: string): JSONObject[] | JSONObject {
    return JSON.parse(readFileSync(filepath).toString());
  }
}
