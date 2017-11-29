// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

const carryBit16 = 1 << 16;

function intAsHex(value: number): string {
    if (value < 0) {
        value = 0xFFFFFFFF + value + 1
    }
    return `0x${value.toString(16)}`;
}

const kInt32DecimalDigits = 8;
const kPowersOfTen = [1,
                      10,
                      100,
                      1000,
                      10000,
                      100000,
                      1000000,
                      10000000,
                      100000000]

export class BaseInt64 {
    constructor (protected buffer: Uint32Array) {}

    high(): number { return this.buffer[1]; }
    low (): number { return this.buffer[0]; }

    protected _times(other: BaseInt64) {
        // Break the left and right numbers into 16 bit chunks
        // so that we can multiply them without overflow.
        const L = new Uint32Array([
            this.buffer[1] >>> 16,
            this.buffer[1] & 0xFFFF,
            this.buffer[0] >>> 16,
            this.buffer[0] & 0xFFFF
        ]);

        const R = new Uint32Array([
            other.buffer[1] >>> 16,
            other.buffer[1] & 0xFFFF,
            other.buffer[0] >>> 16,
            other.buffer[0] & 0xFFFF
        ]);

        let product = L[3] * R[3];
        this.buffer[0] = product & 0xFFFF;

        let sum = product >>> 16;

        product = L[2] * R[3];
        sum += product;

        product = (L[3] * R[2]) >>> 0;
        sum += product;

        this.buffer[0] += sum << 16;

        this.buffer[1] = (sum >>> 0 < product ? carryBit16 : 0);

        this.buffer[1] += sum >>> 16;
        this.buffer[1] += L[1] * R[3] + L[2] * R[2] + L[3] * R[1];
        this.buffer[1] += (L[0] * R[3] + L[1] * R[2] + L[2] * R[1] + L[3] * R[0]) << 16;

        return this;
      }

    protected _plus(other: BaseInt64) {
        const sum = (this.buffer[0] + other.buffer[0]) >>> 0;
        this.buffer[1] += other.buffer[1];
        if (sum < (this.buffer[0] >>> 0)) {
          ++this.buffer[1];
        }
        this.buffer[0] = sum;
    }

    lessThan(other: BaseInt64): boolean {
        return this.buffer[1] < other.buffer[1] ||
            (this.buffer[1] === other.buffer[1] && this.buffer[0] < other.buffer[0]);
    }

    equals(other: BaseInt64): boolean {
        return this.buffer[1] === other.buffer[1] && this.buffer[0] == other.buffer[0];
    }

    greaterThan(other: BaseInt64): boolean {
        return other.lessThan(this);
    }

    hex(): string {
        return `${intAsHex(this.buffer[1])} ${intAsHex(this.buffer[0])}`;
    }
}

export class Uint64 extends BaseInt64 {
    times(other: Uint64): Uint64 {
        this._times(other);
        return this;
    }

    plus(other: Uint64): Uint64 {
        this._plus(other);
        return this;
    }

    static multiply(left: Uint64, right: Uint64): Uint64 {
        let rtrn = new Uint64(new Uint32Array(left.buffer));
        return rtrn.times(right);
    }

    static add(left: Uint64, right: Uint64): Uint64 {
        let rtrn = new Uint64(new Uint32Array(left.buffer));
        return rtrn.plus(right);
    }
}

export class Int64 extends BaseInt64 {
    negate(): Int64 {
        this.buffer[0] = ~this.buffer[0] + 1;
        this.buffer[1] = ~this.buffer[1];

        if (this.buffer[0] == 0) ++this.buffer[1];
        return this;
    }

    times(other: Int64): Int64 {
        this._times(other);
        return this;
    }

    plus(other: Int64): Int64 {
        this._plus(other);
        return this;
    }

    lessThan(other: Int64): boolean {
        // force high bytes to be signed
        const this_high = this.buffer[1] << 0;
        const other_high = other.buffer[1] << 0;
        return this_high < other_high ||
            (this_high === other_high && this.buffer[0] < other.buffer[0]);
    }

    static fromString(str: string): Int64 {
        //DCHECK_NE(out, NULLPTR) << "Decimal128 output variable cannot be NULLPTR";
        //DCHECK_EQ(*out, 0)
            //<< "When converting a string to Decimal128 the initial output must be 0";


        //DCHECK_GT(length, 0) << "length of parsed decimal string should be greater than 0";
        const negate = str.startsWith("-")
        if (negate) str = str.substr(1);
        const length = str.length;

        let out = new Int64(new Uint32Array([0, 0]));
        for (let posn = 0; posn < length;) {
            const group = kInt32DecimalDigits < length - posn ?
                          kInt32DecimalDigits : length - posn;
            const chunk = new Int64(new Uint32Array([parseInt(str.substr(posn, group), 10), 0]));
            const multiple = new Int64(new Uint32Array([kPowersOfTen[group], 0]));

            out.times(multiple);
            out.plus(chunk);

            posn += group;
        }

        return negate ? out.negate() : out;
    }

    static multiply(left: Int64, right: Int64): Int64 {
        let rtrn = new Int64(new Uint32Array(left.buffer));
        return rtrn.times(right);
    }

    static add(left: Int64, right: Int64): Int64 {
        let rtrn = new Int64(new Uint32Array(left.buffer));
        return rtrn.plus(right);
    }
}

export class Int128 {
    constructor (private buffer: Uint32Array) {
        // buffer[3] MSB (high)
        // buffer[2]
        // buffer[1]
        // buffer[0] LSB (low)
    }

    high(): Int64 {
        return new Int64(new Uint32Array(this.buffer.buffer, this.buffer.byteOffset + 8, 2));
    }

    low(): Int64 {
        return new Int64(new Uint32Array(this.buffer.buffer, this.buffer.byteOffset, 2));
    }

    negate(): Int128 {
        this.buffer[0] = ~this.buffer[0] + 1;
        this.buffer[1] = ~this.buffer[1];
        this.buffer[2] = ~this.buffer[2];
        this.buffer[3] = ~this.buffer[3];

        if (this.buffer[0] == 0) ++this.buffer[1];
        if (this.buffer[1] == 0) ++this.buffer[2];
        if (this.buffer[2] == 0) ++this.buffer[3];
        return this;
    }

    times(other: Int128): Int128 {
        // Break the left and right numbers into 32 bit chunks
        // so that we can multiply them without overflow.
        const L0 = new Uint64(new Uint32Array([this.buffer[3],  0]))
        const L1 = new Uint64(new Uint32Array([this.buffer[2],  0]))
        const L2 = new Uint64(new Uint32Array([this.buffer[1],  0]))
        const L3 = new Uint64(new Uint32Array([this.buffer[0],  0]))

        const R0 = new Uint64(new Uint32Array([other.buffer[3], 0]))
        const R1 = new Uint64(new Uint32Array([other.buffer[2], 0]))
        const R2 = new Uint64(new Uint32Array([other.buffer[1], 0]))
        const R3 = new Uint64(new Uint32Array([other.buffer[0], 0]))

        let product = Uint64.multiply(L3, R3);
        this.buffer[0] = product.low();

        let sum = new Uint64(new Uint32Array([product.high(), 0]));

        product = Uint64.multiply(L2, R3);
        sum.plus(product);

        product = Uint64.multiply(L3, R2);
        sum.plus(product);

        this.buffer[1] = sum.low()

        this.buffer[3] = (sum.lessThan(product) ? 1 : 0);

        this.buffer[2] = sum.high();
        let high = new Uint64(new Uint32Array(this.buffer.buffer, this.buffer.byteOffset + 8, 2));

        high.plus(Uint64.multiply(L1, R3))
            .plus(Uint64.multiply(L2, R2))
            .plus(Uint64.multiply(L3, R1));
        this.buffer[3] += Uint64.multiply(L0, R3)
                        .plus(Uint64.multiply(L1, R2))
                        .plus(Uint64.multiply(L2, R1))
                        .plus(Uint64.multiply(L3, R0)).low();

        return this;
    }

    plus(other: Int128): Int128 {
        let sums = new Uint32Array(4);
        sums[3] = (this.buffer[3] + other.buffer[3]) >>> 0;
        sums[2] = (this.buffer[2] + other.buffer[2]) >>> 0;
        sums[1] = (this.buffer[1] + other.buffer[1]) >>> 0;
        sums[0] = (this.buffer[0] + other.buffer[0]) >>> 0;

        if (sums[0] < (this.buffer[0] >>> 0)) {
            ++sums[1];
        }
        if (sums[1] < (this.buffer[1] >>> 0)) {
            ++sums[2];
        }
        if (sums[2] < (this.buffer[2] >>> 0)) {
            ++sums[3];
        }

        this.buffer[3] = sums[3];
        this.buffer[2] = sums[2];
        this.buffer[1] = sums[1];
        this.buffer[0] = sums[0];

        return this;
    }

    hex(): string {
        return `${intAsHex(this.buffer[3])} ${intAsHex(this.buffer[2])} ${intAsHex(this.buffer[1])} ${intAsHex(this.buffer[0])}`;
    }

    static multiply(left: Int128, right: Int128): Int128 {
        let rtrn = new Int128(new Uint32Array(left.buffer));
        return rtrn.times(right);
    }

    static add(left: Int128, right: Int128): Int128 {
        let rtrn = new Int128(new Uint32Array(left.buffer));
        return rtrn.plus(right);
    }

    static fromString(str: string, out_buffer = new Uint32Array(4)): Int128 {
        // TODO: Assert that out_buffer is 0 and length = 4
        const negate = str.startsWith("-");
        const length = str.length;

        let out = new Int128(out_buffer);
        for (let posn = negate ? 1 : 0; posn < length;) {
            const group = kInt32DecimalDigits < length - posn ?
                          kInt32DecimalDigits : length - posn;
            const chunk = new Int128(new Uint32Array([parseInt(str.substr(posn, group), 10), 0, 0, 0]));
            const multiple = new Int128(new Uint32Array([kPowersOfTen[group], 0, 0, 0]));

            out.times(multiple);
            out.plus(chunk);

            posn += group;
        }

        return negate ? out.negate() : out;
    }
}
