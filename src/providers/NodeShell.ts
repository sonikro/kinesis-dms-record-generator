import { exec } from 'child_process';
import { Shell } from '../core/providers/Shell';

export class NodeShell implements Shell {
  execute(command: string): Promise<string> {
    return new Promise((resolve, reject) => {
      exec(command, (error, stdout) => {
        if (error) {
          reject(error);
        } else {
          resolve(stdout);
        }
      });
    });
  }
}
