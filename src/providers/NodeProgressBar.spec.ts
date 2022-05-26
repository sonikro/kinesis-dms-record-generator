import { MultiBar } from 'cli-progress';
import { ProgressBarOptions } from '../core/providers/ProgressBar';
import { NodeProgressBar } from './NodeProgressBar';

describe('NodeProgressBar', () => {
  it('should get a instance of a multi bar', () => {
    // Given
    const multibar = new NodeProgressBar();

    // When
    const response = multibar.getMultiBar();

    // Then
    expect(response).toBeInstanceOf(MultiBar);
    multibar.multiBar.stop();
  });

  it('should create a single bar', () => {
    // Given
    const multibar = new NodeProgressBar();
    const mockedOptions: ProgressBarOptions = {
      total: 10,
      startValue: 0,
      payload: {
        fileName: 'TOTAL PROGRESS',
        dataType: 'Files',
      },
    };
    const spyCreateSingleBar = jest.spyOn(multibar, 'createSingleBar');
    // When
    multibar.createSingleBar(mockedOptions);

    // Then
    expect(spyCreateSingleBar).toHaveBeenCalledWith(mockedOptions);
    multibar.multiBar.stop();
  });

  it('should create a multi bar with cliFormat', () => {
    // Given
    const cliFormat = '{fileName} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total} {dataType}';
    const multibar = new NodeProgressBar();
    // When
    const spyCreateMultiBar = jest.spyOn(multibar, 'createMultiBar');
    multibar.createMultiBar(cliFormat);

    // Then
    expect(spyCreateMultiBar).toHaveBeenCalledWith(cliFormat);
    multibar.multiBar.stop();
  });
});
