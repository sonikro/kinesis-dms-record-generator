import { MultiBar } from 'cli-progress';
import { ProgressBar, ProgressBarOptions } from '../core/providers/ProgressBar';

export class NodeProgressBar implements ProgressBar {
  createMultiBar(options: ProgressBarOptions): MultiBar {
    return new MultiBar(options);
  }
}
