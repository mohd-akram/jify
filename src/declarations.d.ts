declare module 'tiny-lru' {
  class LRU<K, V> {
    get(key: K): V;
    set(key: K, value: V): LRU<K, V>;
  }
  export default function lru<K, V>(size: number): LRU<K, V>
}

declare module 'z85' {
  export function encode(data: Buffer): string
  export function decode(string: string): Buffer
}
