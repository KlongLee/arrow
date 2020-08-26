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

use crate::error::Result;
use crate::logicalplan::ScalarValue;
use crate::variable::VarProvider;

pub struct SystemVar {}

impl SystemVar {
    pub fn new() -> Self {
        Self {}
    }
}

impl VarProvider for SystemVar {
    fn get_value(&self, var_names: Vec<String>) -> Result<ScalarValue> {
        let s = format!("{}-{}", "test".to_string(), var_names.concat());
        Ok(ScalarValue::Utf8(s))
    }
}
