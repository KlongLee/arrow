import { validateVector } from './utils';
import { Vector, DateDay, DateMillisecond } from '../../Arrow';
import {
    encodeAll,
    encodeEach,
    date32sNoNulls,
    date64sNoNulls,
    date32sWithNulls,
    date64sWithNulls
} from './utils';

describe('DateDayBuilder', () => {

    runTestsWithEncoder('encodeAll', encodeAll(() => new DateDay()));
    runTestsWithEncoder('encodeEach chunkLength: 5', encodeEach(() => new DateDay(), 5));
    runTestsWithEncoder('encodeEach chunkLength: 25', encodeEach(() => new DateDay(), 25));
    runTestsWithEncoder('encodeEach chunkLength: undefined', encodeEach(() => new DateDay()));
    
    function runTestsWithEncoder(name: string, encode: (vals: (Date | null)[], nullVals?: any[]) => Vector<DateDay>) {
        describe(`${encode.name} ${name}`, () => {
            it(`encodes dates no nulls`, () => {
                const vals = date32sNoNulls(20);
                validateVector(vals, encode(vals, []), []);
            });
            it(`encodes dates with nulls`, () => {
                const vals = date32sWithNulls(20);
                validateVector(vals, encode(vals, [null]), [null]);
            });
        });
    }
});

describe('DateMillisecondBuilder', () => {

    runTestsWithEncoder('encodeAll', encodeAll(() => new DateMillisecond()));
    runTestsWithEncoder('encodeEach: 5', encodeEach(() => new DateMillisecond(), 5));
    runTestsWithEncoder('encodeEach: 25', encodeEach(() => new DateMillisecond(), 25));
    runTestsWithEncoder('encodeEach: undefined', encodeEach(() => new DateMillisecond()));

    function runTestsWithEncoder(name: string, encode: (vals: (Date | null)[], nullVals?: any[]) => Vector<DateMillisecond>) {
        describe(`${encode.name} ${name}`, () => {
            it(`encodes dates no nulls`, () => {
                const vals = date64sNoNulls(20);
                validateVector(vals, encode(vals, []), []);
            });
            it(`encodes dates with nulls`, () => {
                const vals = date64sWithNulls(20);
                validateVector(vals, encode(vals, [null]), [null]);
            });
        });
    }
});

describe('DateMillisecondBuilder', () => {
    const encode = encodeAll(() => new DateMillisecond());
    const dates = [
        null,
        "2019-03-19T13:40:14.746Z",
        "2019-03-06T21:12:50.912Z",
        "2019-03-22T12:50:56.854Z",
        "2019-02-25T03:34:30.916Z",
        null,
        null,
        null,
        null,
        null,
        null, 
        "2019-03-18T18:12:37.293Z", 
        "2019-03-26T21:58:35.307Z", 
        "2019-04-02T03:03:46.464Z", 
        "2019-03-24T18:45:25.763Z",
        null, 
        "2019-03-19T01:10:59.189Z",
        "2019-03-10T21:15:32.237Z",
        "2019-03-21T07:25:34.864Z",
        null
    ].map((x) => x === null ? x : new Date(x));
    it(`encodes dates with nulls`, () => {
        const vals = dates.slice();
        validateVector(vals, encode(vals, [null]), [null]);
    });
});
