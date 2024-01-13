export function predicate<T>(
  strings: TemplateStringsArray,
  ...values: T[]
): Predicate<T> {
  let ops = strings.filter((o) => o);

  if (!ops.length || ops.length > 2 || ops.length != values.length)
    throw new Error("Invalid predicate");

  let start: T | undefined;
  let end: T | undefined;

  let excludeStart = false;
  let excludeEnd = false;

  ops = ops.map((o, i) => {
    const parts = o.trim().split(/\s+/);

    if (parts.length > 2) throw new Error("Invalid operator");
    if (parts.length > 1 && parts[0] != "&&")
      throw new Error("Only `&&` is supported between conditions");

    const op = parts[parts.length - 1];

    if (!["<", ">", "<=", ">="].includes(op))
      throw new Error(`Unsupported operator "${op}" in predicate`);

    if (op.startsWith(">")) {
      if (start != undefined)
        throw new Error("Redundant operator in predicate");
      excludeStart = op == ">";
      start = values[i];
    }

    if (op.startsWith("<")) {
      if (end != undefined) throw new Error("Redundant operator in predicate");
      excludeEnd = op == "<";
      end = values[i];
    }

    return op;
  });

  let converted = false;

  const test: Predicate<T> = function (value: T) {
    if (test.key && !converted) {
      if (start != undefined) start = test.key(start);
      if (end != undefined) end = test.key(end);
      converted = true;
    }
    const seek =
      start == undefined ? 1 : value < start ? -1 : value > start ? 1 : 0;
    const match =
      (start == undefined || (excludeStart ? value > start : value >= start)) &&
      (end == undefined || (excludeEnd ? value < end : value <= end));
    return { seek, match };
  };

  return test;
}

export interface Predicate<T> {
  key?: (value: T) => any;
  (value: T): { seek: number; match: boolean };
}

export interface Query {
  [field: string]: any | Predicate<any>;
}
