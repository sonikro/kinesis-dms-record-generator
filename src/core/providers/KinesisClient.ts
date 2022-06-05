export interface KinesisClient {
  send(payload: Record<string, any>[]): Promise<void>;
}
