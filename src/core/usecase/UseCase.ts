export interface UseCase<Input, Output> {
  invoke(input: Input): Output;
}
