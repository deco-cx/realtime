import type { DeleteAtOperation, TextFilePatchOperation } from "../realtime.ts";
import { BinaryIndexedTree } from "./bit.ts";

const shrinkOperations = (
  operations: TextFilePatchOperation[],
): TextFilePatchOperation[] => {
  const groupedOperations: TextFilePatchOperation[] = [];

  let currentOperation: TextFilePatchOperation | undefined = operations[0];
  let lastIdx = currentOperation?.at;
  for (const operation of operations.slice(1)) {
    const isConsecutive = operation.at - 1 === lastIdx;
    if (isConsecutive) {
      if (
        "length" in currentOperation && "length" in operation
      ) {
        currentOperation.length += operation.length;
        lastIdx++;
        continue;
      } else if ("text" in currentOperation && "text" in operation) {
        currentOperation.text += operation.text;
        lastIdx++;
        continue;
      }
    }
    groupedOperations.push(currentOperation);
    currentOperation = operation;
    lastIdx = currentOperation.at;
  }

  // Add the last operation to the grouped operations list
  if (currentOperation) {
    groupedOperations.push(currentOperation);
  }

  return groupedOperations;
};
// GENERATED BY GPT 4.0
export const diff = (
  oldStr: string,
  newStr: string,
): TextFilePatchOperation[] => {
  const m = oldStr.length;
  const n = newStr.length;
  const dp: number[][] = Array.from(
    { length: m + 1 },
    () => Array(n + 1).fill(0),
  );

  // Building the DP matrix
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      if (oldStr[i - 1] === newStr[j - 1]) {
        dp[i][j] = dp[i - 1][j - 1] + 1;
      } else {
        dp[i][j] = Math.max(dp[i - 1][j], dp[i][j - 1]);
      }
    }
  }

  const operations: TextFilePatchOperation[] = [];
  let i = m, j = n;

  // Trace back from dp[m][n]
  while (i > 0 || j > 0) {
    if (i > 0 && j > 0 && oldStr[i - 1] === newStr[j - 1]) {
      i--;
      j--;
    } else if (j > 0 && (i === 0 || dp[i][j - 1] >= dp[i - 1][j])) {
      let insertionLength = 1;
      while (
        j - insertionLength > 0 &&
        newStr[j - insertionLength - 1] === newStr[j - 1]
      ) {
        insertionLength++;
      }
      operations.unshift({
        at: i,
        text: newStr.substr(j - insertionLength, insertionLength),
      });
      j -= insertionLength;
    } else {
      operations.unshift({ at: i - 1, length: 1 });
      i--;
    }
  }

  return shrinkOperations(operations);
};

const isDeleteOperation = (
  op: TextFilePatchOperation,
): op is DeleteAtOperation => {
  return (op as DeleteAtOperation).length !== undefined;
};

export const applyPatch =
  (offsets: BinaryIndexedTree, rollbacks: Array<() => void>) =>
  (
    [str, success]: [string, boolean],
    op: TextFilePatchOperation,
  ): [string, boolean] => {
    if (!success) {
      return [str, success];
    }
    if (isDeleteOperation(op)) {
      const { at, length } = op;
      const offset = offsets.query(at) + at;
      if (offset < 0) {
        return [str, false];
      }
      const before = str.slice(0, offset);
      const after = str.slice(offset + length);

      // Update BIT for deletion operation
      offsets.update(at, -length); // Subtract length from the index
      rollbacks.push(() => {
        offsets.update(at, length);
      });
      return [`${before}${after}`, true];
    }
    const { at, text } = op;
    const offset = offsets.query(at) + at;
    if (offset < 0) {
      return [str, false];
    }

    const before = str.slice(0, offset);
    const after = str.slice(offset); // Use offset instead of at

    // Update BIT for insertion operation
    offsets.update(at, text.length); // Add length of text at the index
    rollbacks.push(() => {
      offsets.update(at, -text.length);
    });
    return [`${before}${text}${after}`, true];
  };

export const apply = (
  str: string,
  ops: TextFilePatchOperation[],
  offsets?: BinaryIndexedTree,
): [string, boolean] => {
  const rollbacks: Array<() => void> = [];
  const [result, success] = ops.reduce(
    applyPatch(offsets ?? new BinaryIndexedTree(), rollbacks),
    [str, true],
  );
  if (!success) {
    rollbacks.forEach((rb) => rb());
  }

  return [result, success];
};
