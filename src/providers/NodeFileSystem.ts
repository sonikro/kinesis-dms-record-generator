import { readdirSync, readFileSync } from 'fs';
import { FileSystem } from '../core/providers/FileSystem';
import { JSONObject } from '../core/domain/JSONObject';

export class NodeFileSystem implements FileSystem {
  readDir(dirname: string): string[] {
    return readdirSync(dirname);
  }

  readJsonFile(filepath: string): JSONObject[] | JSONObject {
    return JSON.parse(readFileSync(filepath).toString());
  }
}
