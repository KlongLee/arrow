﻿// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections.Generic;
using System.Runtime.InteropServices;
using Apache.Arrow.Acero.CLib;

namespace Apache.Arrow.Acero
{
    public class SinkNode : ExecNode
    {
        private readonly unsafe GArrowExecuteNode* _nodePtr;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        protected unsafe delegate GArrowExecuteNode* d_garrow_execute_plan_build_sink_node(GArrowExecutePlan* plan, GArrowExecuteNode* input, GArrowSinkNodeOptions* options, out GError** error);
        protected static d_garrow_execute_plan_build_sink_node garrow_execute_plan_build_sink_node = FuncLoader.LoadFunction<d_garrow_execute_plan_build_sink_node>("garrow_execute_plan_build_sink_node");

        public override unsafe GArrowExecuteNode* Handle => _nodePtr;

        public unsafe SinkNode(SinkNodeOptions _options, ExecPlan plan, List<ExecNode> inputs)
        {
            _nodePtr = garrow_execute_plan_build_sink_node(plan.Handle, inputs[0].Handle, _options.Handle, out GError** error);

            ExceptionUtil.ThrowOnError(error);
        }
    }
}
