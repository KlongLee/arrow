// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compute

import (
	"sync"

	"github.com/apache/arrow/go/v10/arrow/internal/debug"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type FunctionOptionsType interface {
	TypeName() string
	Compare(lhs, rhs FunctionOptions) bool
	Copy(FunctionOptions) FunctionOptions
}

type FunctionRegistry interface {
	CanAddFunction(fn Function, allowOverwrite bool) bool
	AddFunction(fn Function, allowOverwrite bool) bool
	CanAddAlias(target, source string) bool
	AddAlias(target, source string) bool
	CanAddFunctionOptionsType(opts FunctionOptionsType, allowOverwrite bool) bool
	AddFunctionOptionsType(opts FunctionOptionsType, allowOverwrite bool) bool
	GetFunction(name string) (Function, bool)
	GetFunctionNames() []string
	GetFunctionOptionsType(name string) (FunctionOptionsType, bool)
	NumFunctions() int

	canAddFuncName(string, bool) bool
	canAddOptionsTypeName(name string, allowOverwrite bool) bool
}

var (
	registry FunctionRegistry
	once     sync.Once
)

func GetFunctionRegistry() FunctionRegistry {
	once.Do(func() {
		registry = NewRegistry()
		// initialize the others
	})
	return registry
}

func NewRegistry() FunctionRegistry {
	return &funcRegistry{
		nameToFunction: make(map[string]Function),
		nameToOptsType: make(map[string]FunctionOptionsType)}
}

func NewChildRegistry(parent FunctionRegistry) FunctionRegistry {
	return &funcRegistry{
		parent:         parent.(*funcRegistry),
		nameToFunction: make(map[string]Function),
		nameToOptsType: make(map[string]FunctionOptionsType)}
}

type funcRegistry struct {
	parent *funcRegistry

	mx             sync.RWMutex
	nameToFunction map[string]Function
	nameToOptsType map[string]FunctionOptionsType
}

func (reg *funcRegistry) getLocker(add bool) sync.Locker {
	if add {
		return &reg.mx
	}
	return reg.mx.RLocker()
}

func (reg *funcRegistry) CanAddFunction(fn Function, allowOverwrite bool) bool {
	if reg.parent != nil && !reg.parent.CanAddFunction(fn, allowOverwrite) {
		return false
	}

	return reg.doAddFunction(fn, allowOverwrite, false)
}

func (reg *funcRegistry) AddFunction(fn Function, allowOverwrite bool) bool {
	if reg.parent != nil && !reg.parent.CanAddFunction(fn, allowOverwrite) {
		return false
	}

	return reg.doAddFunction(fn, allowOverwrite, true)
}

func (reg *funcRegistry) CanAddAlias(target, source string) bool {
	if reg.parent != nil && !reg.parent.canAddFuncName(target, false) {
		return false
	}
	return reg.doAddAlias(target, source, false)
}

func (reg *funcRegistry) AddAlias(target, source string) bool {
	if reg.parent != nil && !reg.parent.canAddFuncName(target, false) {
		return false
	}

	return reg.doAddAlias(target, source, true)
}

func (reg *funcRegistry) CanAddFunctionOptionsType(opts FunctionOptionsType, allowOverwrite bool) bool {
	if reg.parent != nil && !reg.parent.CanAddFunctionOptionsType(opts, allowOverwrite) {
		return false
	}

	return reg.doAddFuncOptionsType(opts, allowOverwrite, false)
}

func (reg *funcRegistry) AddFunctionOptionsType(opts FunctionOptionsType, allowOverwrite bool) bool {
	if reg.parent != nil && !reg.parent.CanAddFunctionOptionsType(opts, allowOverwrite) {
		return false
	}

	return reg.doAddFuncOptionsType(opts, allowOverwrite, true)
}

func (reg *funcRegistry) GetFunction(name string) (Function, bool) {
	reg.mx.RLock()
	defer reg.mx.RUnlock()

	if fn, ok := reg.nameToFunction[name]; ok {
		return fn, ok
	}

	if reg.parent != nil {
		return reg.parent.GetFunction(name)
	}

	return nil, false
}

func (reg *funcRegistry) GetFunctionNames() (out []string) {
	if reg.parent != nil {
		out = reg.parent.GetFunctionNames()
	} else {
		out = make([]string, 0, len(reg.nameToFunction))
	}
	reg.mx.RLock()
	defer reg.mx.RUnlock()

	out = append(out, maps.Keys(reg.nameToFunction)...)
	slices.Sort(out)
	return
}

func (reg *funcRegistry) GetFunctionOptionsType(name string) (FunctionOptionsType, bool) {
	reg.mx.RLock()
	defer reg.mx.RUnlock()

	opts, ok := reg.nameToOptsType[name]
	if ok {
		return opts, true
	}

	if reg.parent != nil {
		return reg.parent.GetFunctionOptionsType(name)
	}
	return nil, false
}

func (reg *funcRegistry) NumFunctions() (n int) {
	if reg.parent != nil {
		n = reg.parent.NumFunctions()
	}
	return n + len(reg.nameToFunction)
}

func (reg *funcRegistry) canAddFuncName(name string, allowOverwrite bool) bool {
	if reg.parent != nil && !reg.parent.canAddFuncName(name, allowOverwrite) {
		return false
	}
	if !allowOverwrite {
		_, ok := reg.nameToFunction[name]
		return !ok
	}
	return true
}

func (reg *funcRegistry) doAddFunction(fn Function, allowOverwrite bool, add bool) bool {
	debug.Assert(fn.Validate() == nil, "invalid function")

	lk := reg.getLocker(add)
	lk.Lock()
	defer lk.Unlock()

	name := fn.Name()
	if !reg.canAddFuncName(name, allowOverwrite) {
		return false
	}

	if add {
		reg.nameToFunction[name] = fn
	}
	return true
}

func (reg *funcRegistry) doAddAlias(target, source string, add bool) bool {
	// source name must exist in the registry or the parent
	// check outside the mutex, in case GetFunction has a mutex
	// acquisition
	fn, ok := reg.GetFunction(source)
	if !ok {
		return false
	}

	lk := reg.getLocker(add)
	lk.Lock()
	defer lk.Unlock()

	if !reg.canAddFuncName(target, false) {
		return false
	}

	if add {
		reg.nameToFunction[target] = fn
	}
	return true
}

func (reg *funcRegistry) canAddOptionsTypeName(name string, allowOverwrite bool) bool {
	if reg.parent != nil && !reg.parent.canAddOptionsTypeName(name, allowOverwrite) {
		return false
	}

	if !allowOverwrite {
		_, ok := reg.nameToOptsType[name]
		return !ok
	}
	return true
}

func (reg *funcRegistry) doAddFuncOptionsType(opts FunctionOptionsType, allowOverwrite, add bool) bool {
	lk := reg.getLocker(add)
	lk.Lock()
	defer lk.Unlock()

	name := opts.TypeName()
	if !reg.canAddOptionsTypeName(name, false) {
		return false
	}

	if add {
		reg.nameToOptsType[name] = opts
	}
	return true
}
