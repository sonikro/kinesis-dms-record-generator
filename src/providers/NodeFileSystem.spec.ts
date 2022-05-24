import * as fs from 'fs';
import { mockDirContent, mockJsonPayloads } from '../test/mocks';
import { NodeFileSystem } from './NodeFileSystem';

describe('NodeFileSystem', () => {
  it('should read a directory and return a list of files', () => {
    // Given
    const mockedTestDir = 'test';

    const readDirMock = jest.spyOn(NodeFileSystem.prototype, 'readDir').mockImplementation(() => {
      return mockDirContent;
    });

    // When
    const fileSystem = new NodeFileSystem();
    fileSystem.readDir(mockedTestDir);

    // Then
    expect(readDirMock).toHaveBeenCalled();
  });

  it('should read JSON file from a full file path', () => {
    // Given
    const mockedTestFile = 'test/1.dbo.test.json';
    const readJsonFileMock = jest
      .spyOn(NodeFileSystem.prototype, 'readJsonFile')
      .mockImplementation(() => {
        return mockJsonPayloads;
      });

    // When
    const fileSystem = new NodeFileSystem();
    fileSystem.readJsonFile(mockedTestFile);

    // Then
    expect(readJsonFileMock).toHaveBeenCalled();
  });
});
