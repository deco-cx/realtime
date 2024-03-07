export class BinaryIndexedTree {
  bit: number[];

  constructor(size: number) {
    this.bit = new Array(size + 1).fill(0);
  }

  // Updates the value at index i by adding delta to it
  update(idx: number, delta: number): void {
    idx++; // Convert 0-based indexing to 1-based indexing
    while (idx < this.bit.length) {
      this.bit[idx] += delta;
      idx += idx & -idx; // Move to next index
    }
  }

  // Returns the sum of values in the range [0, i]
  query(r: number): number {
    r++; // Convert 0-based indexing to 1-based indexing
    let ret = 0;
    while (r > 0) {
      ret += this.bit[r];
      r -= r & -r; // Move to parent index
    }
    return ret;
  }

  // Returns the sum of values in the range [left, right]
  rangeQuery(left: number, right: number): number {
    return this.query(right) - this.query(left - 1);
  }
}
