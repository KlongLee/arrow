package array

import (
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

// Diff compares two arrays, returning an edit script which expresses the difference
// between them. The edit script can be applied to the base array to produce the target.
//
// An edit script is an array of struct(insert bool, run_length int64).
// Each element of "insert" determines whether an element was inserted into (true)
// or deleted from (false) base. Each insertion or deletion is followed by a run of
// elements which are unchanged from base to target; the length of this run is stored
// in "run_length". (Note that the edit script begins and ends with a run of shared
// elements but both fields of the struct must have the same length. To accommodate this
// the first element of "insert" should be ignored.)
//
// For example for base "hlloo" and target "hello", the edit script would be
// [
//
//	{"insert": false, "run_length": 1}, // leading run of length 1 ("h")
//	{"insert": true, "run_length": 3}, // insert("e") then a run of length 3 ("llo")
//	{"insert": false, "run_length": 0} // delete("o") then an empty run
//
// ]
//
// base: baseline for comparison
// target: an array of identical type to base whose elements differ from base's
// mem: memory to store the result will be allocated from this memory pool
func Diff(base, target arrow.Array, mem memory.Allocator) (*Struct, error) {
	if base.DataType().Fingerprint() != target.DataType().Fingerprint() {
		return nil, errors.New("only taking the diff of like-typed arrays is supported")
	}
	switch base.DataType().ID() {
	case arrow.EXTENSION:
		return Diff(base.(ExtensionArray).Storage(), target.(ExtensionArray).Storage(), mem)
	case arrow.DICTIONARY:
		return nil, fmt.Errorf("diffing arrays of type %s is not implemented", base.DataType().String())
	case arrow.RUN_END_ENCODED:
		return nil, fmt.Errorf("diffing arrays of type %s is not implemented", base.DataType().String())
	}
	d := newQuadraticSpaceMyersDiff(base, target)
	r, err := d.Diff(mem)
	if err != nil {
		if r != nil {
			r.Release()
		}
		return nil, err
	}
	return r, nil
}

// editPoint represents an intermediate state in the comparison of two arrays
type editPoint struct {
	base   int
	target int
}

func (e editPoint) Equals(other editPoint) bool {
	return e.base == other.base && e.target == other.target
}

type quadraticSpaceMyersDiff struct {
	base         arrow.Array
	target       arrow.Array
	finishIndex  int
	editCount    int
	endpointBase []int
	insert       []bool
	baseBegin    int
	targetBegin  int
	baseEnd      int
	targetEnd    int
}

func newQuadraticSpaceMyersDiff(base, target arrow.Array) *quadraticSpaceMyersDiff {
	d := &quadraticSpaceMyersDiff{
		base:         base,
		target:       target,
		finishIndex:  -1,
		editCount:    0,
		endpointBase: []int{},
		insert:       []bool{},
		baseBegin:    0,
		targetBegin:  0,
		baseEnd:      base.Len(),
		targetEnd:    target.Len(),
	}
	d.endpointBase = []int{d.extendFrom(editPoint{d.baseBegin, d.targetBegin}).base}
	if d.baseEnd-d.baseBegin == d.targetEnd-d.targetBegin && d.endpointBase[0] == d.baseEnd {
		// trivial case: base == target
		d.finishIndex = 0
	}
	return d
}

func (d *quadraticSpaceMyersDiff) valuesEqual(baseIndex, targetIndex int) bool {
	baseNull := d.base.IsNull(baseIndex)
	targetNull := d.target.IsNull(targetIndex)
	if baseNull || targetNull {
		return baseNull && targetNull
	}
	return SliceEqual(d.base, int64(baseIndex), int64(baseIndex+1), d.target, int64(targetIndex), int64(targetIndex+1))
}

// increment the position within base and target (the elements skipped in this way were
// present in both sequences)
func (d *quadraticSpaceMyersDiff) extendFrom(p editPoint) editPoint {
	for p.base != d.baseEnd && p.target != d.targetEnd {
		if !d.valuesEqual(p.base, p.target) {
			break
		}
		p.base++
		p.target++
	}
	return p
}

// increment the position within base (the element pointed to was deleted)
// then extend maximally
func (d *quadraticSpaceMyersDiff) deleteOne(p editPoint) editPoint {
	if p.base != d.baseEnd {
		p.base++
	}
	return d.extendFrom(p)
}

// increment the position within target (the element pointed to was inserted)
// then extend maximally
func (d *quadraticSpaceMyersDiff) insertOne(p editPoint) editPoint {
	if p.target != d.targetEnd {
		p.target++
	}
	return d.extendFrom(p)
}

// beginning of a range for storing per-edit state in endpointBase and insert
func storageOffset(editCount int) int {
	return editCount * (editCount + 1) / 2
}

// given edit_count and index, augment endpointBase[index] with the corresponding
// position in target (which is only implicitly represented in editCount, index)
func (d *quadraticSpaceMyersDiff) getEditPoint(editCount, index int) editPoint {
	insertionsMinusDeletions := 2*(index-storageOffset(editCount)) - editCount
	maximalBase := d.endpointBase[index]
	maximalTarget := min(d.targetBegin+((maximalBase-d.baseBegin)+insertionsMinusDeletions), d.targetEnd)
	return editPoint{maximalBase, maximalTarget}
}

func (d *quadraticSpaceMyersDiff) Next() {
	d.editCount++
	if len(d.endpointBase) < storageOffset(d.editCount+1) {
		d.endpointBase = append(d.endpointBase, make([]int, storageOffset(d.editCount+1)-len(d.endpointBase))...)
	}
	if len(d.insert) < storageOffset(d.editCount+1) {
		d.insert = append(d.insert, make([]bool, storageOffset(d.editCount+1)-len(d.insert))...)
	}
	previousOffset := storageOffset(d.editCount - 1)
	currentOffset := storageOffset(d.editCount)

	// try deleting from base first
	for i, iOut := 0, 0; i < d.editCount; i, iOut = i+1, iOut+1 {
		previousEndpoint := d.getEditPoint(d.editCount-1, i+previousOffset)
		d.endpointBase[iOut+currentOffset] = d.deleteOne(previousEndpoint).base
	}

	// check if inserting from target could do better
	for i, iOut := 0, 1; i < d.editCount; i, iOut = i+1, iOut+1 {
		// retrieve the previously computed best endpoint for (editCount, iOut)
		// for comparison with the best endpoint achievable with an insertion
		endpointAfterDeletion := d.getEditPoint(d.editCount, iOut+currentOffset)

		previousEndpoint := d.getEditPoint(d.editCount-1, i+previousOffset)
		endpointAfterInsertion := d.insertOne(previousEndpoint)

		if endpointAfterInsertion.base-endpointAfterDeletion.base >= 0 {
			// insertion was more efficient; keep it and mark the insertion in insert
			d.insert[iOut+currentOffset] = true
			d.endpointBase[iOut+currentOffset] = endpointAfterInsertion.base
		}
	}

	finish := editPoint{d.baseEnd, d.targetEnd}
	for iOut := 0; iOut < d.editCount+1; iOut++ {
		if d.getEditPoint(d.editCount, iOut+currentOffset) == finish {
			d.finishIndex = iOut + currentOffset
			return
		}
	}
}

func (d *quadraticSpaceMyersDiff) Done() bool {
	return d.finishIndex != -1
}

func (d *quadraticSpaceMyersDiff) GetEdits(mem memory.Allocator) (*Struct, error) {
	if !d.Done() {
		panic("GetEdits called but Done() = false")
	}

	length := d.editCount + 1
	insertBuf := make([]bool, length)
	runLength := make([]int64, length)
	index := d.finishIndex
	endpoint := d.getEditPoint(d.editCount, d.finishIndex)

	for i := d.editCount; i > 0; i-- {
		insert := d.insert[index]
		insertBuf[i] = insert
		insertionsMinusDeletions := (endpoint.base - d.baseBegin) - (endpoint.target - d.targetBegin)
		if insert {
			insertionsMinusDeletions++
		} else {
			insertionsMinusDeletions--
		}
		index = (i-1-insertionsMinusDeletions)/2 + storageOffset(i-1)

		// endpoint of previous edit
		previous := d.getEditPoint(i-1, index)
		in := 0
		if insert {
			in = 1
		}
		runLength[i] = int64(endpoint.base - previous.base - (1 - in))
		endpoint = previous
	}
	insertBuf[0] = false
	runLength[0] = int64(endpoint.base - d.baseBegin)

	inserts := NewBooleanBuilder(mem)
	defer inserts.Release()
	inserts.AppendValues(insertBuf, nil)

	run := NewInt64Builder(mem)
	defer run.Release()
	run.AppendValues(runLength, nil)

	insArr := inserts.NewArray()
	defer insArr.Release()

	runArr := run.NewArray()
	defer runArr.Release()

	return NewStructArray([]arrow.Array{
		insArr,
		runArr,
	}, []string{"insert", "run_length"})
}

func (d *quadraticSpaceMyersDiff) Diff(mem memory.Allocator) (*Struct, error) {
	for !d.Done() {
		d.Next()
	}
	return d.GetEdits(mem)
}

// DiffString returns a representation of the diff in Unified Diff format.
func DiffString(base, target arrow.Array, mem memory.Allocator) (string, error) {
	edits, err := Diff(base, target, mem)
	if err != nil {
		return "", err
	}
	defer edits.Release()

	inserts := edits.Field(0).(*Boolean)
	runLengths := edits.Field(1).(*Int64)

	var s strings.Builder
	baseIndex := int64(0)
	targetIndex := int64(0)
	wrotePosition := false
	for i := 0; i < runLengths.Len(); i++ {
		if i > 0 {
			if !wrotePosition {
				s.WriteString(fmt.Sprintf("@@ -%d, +%d @@\n", baseIndex, targetIndex))
				wrotePosition = true
			}
			if inserts.Value(i) {
				s.WriteString(fmt.Sprintf("+%v\n", stringAt(target, targetIndex)))
				targetIndex++
			} else {
				s.WriteString(fmt.Sprintf("-%v\n", stringAt(base, baseIndex)))
				baseIndex++
			}
		}
		for j := int64(0); j < runLengths.Value(i); j++ {
			baseIndex += 1
			targetIndex += 1
			wrotePosition = false
		}
	}
	return s.String(), nil
}

func stringAt(arr arrow.Array, i int64) string {
	if arr.IsNull(int(i)) {
		return "null"
	}

	s := NewSlice(arr, i, i+1)
	defer s.Release()
	st := s.String()
	return st[1 : len(st)-1]
}
