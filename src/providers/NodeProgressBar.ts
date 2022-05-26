import { MultiBar, Presets, SingleBar } from 'cli-progress';
import { ProgressBar, ProgressBarOptions } from '../core/providers/ProgressBar';

export class NodeProgressBar implements ProgressBar {
  readonly multiBar: MultiBar;

  constructor(cliFormat?: string) {
    this.multiBar = this.createMultiBar(cliFormat);
  }

  getMultiBar(): MultiBar {
    return this.multiBar;
  }

  createSingleBar(options: ProgressBarOptions): SingleBar {
    const { total, startValue, payload, barOptions } = options;
    return this.multiBar.create(total, startValue, payload, barOptions);
  }

  createMultiBar(cliFormat?: string): MultiBar {
    const defaultFormat =
      '{fileName} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total} {dataType}';
    const format = cliFormat ?? defaultFormat;
    return new MultiBar({ format }, Presets.shades_classic);
  }
}
