export default interface Store<T> {
  get(position: number): Promise<{
    value: T;
    start: number;
    length: number;
  }>;
  create(): Promise<void>;
  destroy(): Promise<void>;
}
