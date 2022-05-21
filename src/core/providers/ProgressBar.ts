import { MultiBar, Options, SingleBar } from 'cli-progress';

export type ProgressBarOptions = {
  total: number;
  startValue: number;
  payload?: any;
  barOptions?: Options;
};

export interface ProgressBar {
  getMultiBar(): MultiBar;
  createSingleBar(options: ProgressBarOptions): SingleBar;
}
