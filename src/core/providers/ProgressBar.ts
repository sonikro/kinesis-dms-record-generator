import { MultiBar, Preset } from 'cli-progress';

export type ProgressBarOptions = {
  format: string;
  preset: Preset;
};

export interface ProgressBar {
  createMultiBar(options: ProgressBarOptions): MultiBar;
}
