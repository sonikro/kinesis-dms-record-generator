export interface Shell {
  execute(command: string): Promise<string>;
}
