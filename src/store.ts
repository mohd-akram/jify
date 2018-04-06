interface Store<T> {
  get(position: number): Promise<{
    value: T, start: number, length: number
  }>;
  insert(data: T, position?: number): Promise<{
    start: number, length: number
  }>;
  remove(position: number): Promise<void>;
  create(): Promise<void>;
  destroy(): Promise<void>;
}
