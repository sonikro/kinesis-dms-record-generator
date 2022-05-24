import * as fs from 'fs';
import { mockDirContent, mockJsonPayloads } from '../test/mocks';
import { NodeFileSystem } from './NodeFileSystem';

jest.mock('fs', () => ({
  readdirSync: jest.fn().mockImplementation(() => mockDirContent),
  readFileSync: jest.fn().mockImplementation(() => Buffer.from(JSON.stringify(mockJsonPayloads))),
}));

describe('NodeFileSystem', () => {
  it('should read a directory and return a list of files', () => {
    // Given
    const mockedTestDir = 'test';
    const spyReaddirSync = jest.spyOn(fs, 'readdirSync');
    // When
    const fileSystem = new NodeFileSystem();
    const response = fileSystem.readDir(mockedTestDir);
    // Then
    expect(spyReaddirSync).toHaveBeenCalledWith(mockedTestDir);
    expect(response).toEqual(mockDirContent);
  });

  it('should read JSON file from a full file path', () => {
    // Given
    const mockedTestFile = 'test/1.dbo.test.json';
    const spyReadFileSync = jest.spyOn(fs, 'readFileSync');
    // When
    const fileSystem = new NodeFileSystem();
    const file = fileSystem.readJsonFile(mockedTestFile);
    // Then
    expect(spyReadFileSync).toHaveBeenCalledWith(mockedTestFile);
    expect(file).toEqual(mockJsonPayloads);
  });
});
