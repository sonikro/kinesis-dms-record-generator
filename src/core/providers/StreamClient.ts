export interface StreamClient {
  send(payload: Record<string, any>[]): Promise<void>;
}
