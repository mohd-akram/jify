declare module 'z85' {
  export function encode(data: Buffer): string
  export function decode(string: string): Buffer
}
