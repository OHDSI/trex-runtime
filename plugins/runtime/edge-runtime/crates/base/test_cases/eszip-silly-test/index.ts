import isEven from "npm:is-even";

console.log("Hello A");
const isTenEven = isEven(10);
console.log("Hello");

if (isTenEven !== true) {
  throw new Error(`Expected isEven(10) to be true, got: ${isTenEven}`);
}

console.log("eszip-silly-test passed");
