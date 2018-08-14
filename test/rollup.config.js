import typescript from 'rollup-plugin-typescript3';

export default {
  input: 'index.ts',
  output: {
    format: 'cjs'
  },
  plugins: [
    typescript({ compilerOptions: { target: 'esnext' } })
  ]
};
