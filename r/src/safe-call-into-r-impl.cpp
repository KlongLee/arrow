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

#include "./arrow_types.h"
#include "./safe-call-into-r.h"

#include <csignal>
#include <functional>
#include <thread>

// for SignalInterruptCondition()
#ifdef _WIN32
#include <Rembedded.h>
#else
#include <Rinterface.h>
#endif

MainRThread& GetMainRThread() {
  static MainRThread main_r_thread;
  return main_r_thread;
}

// [[arrow::export]]
void InitializeMainRThread() { GetMainRThread().Initialize(); }

bool CanRunWithCapturedR() {
#if defined(HAS_UNWIND_PROTECT)
  static int on_old_windows = -1;
  if (on_old_windows == -1) {
    cpp11::function on_old_windows_fun = cpp11::package("arrow")["on_old_windows"];
    on_old_windows = on_old_windows_fun();
  }

  return !on_old_windows;
#else
  return false;
#endif
}

void SignalInterruptCondition() {
#ifdef _WIN32
  UserBreak = 1;
  R_CheckUserInterrupt();
#else
  Rf_onintr();
#endif
}

void OverridingSignalHandler(int sig) {
  auto main_r_thread = GetMainRThread();

  if (!main_r_thread.IsExecutingSafeCallIntoR() && !main_r_thread.HasError() &&
      sig == SIGINT) {
    main_r_thread.RequestStopFromSignal(sig);
    main_r_thread.SetError(arrow::Status::Cancelled("User interrupt"));
  } else {
    main_r_thread.CallPreviousSignalHandler(sig);
  }
}

void MainRThread::SetOverrideInterruptSignal(bool enabled) {
  bool was_enabled = IsOverridingInterruptSignal();
  if (enabled && !was_enabled) {
    // enable override
    previous_signal_handler_ = signal(SIGINT, &OverridingSignalHandler);
    stop_source_ = arrow::ValueOrStop(arrow::SetSignalStopSource());
    arrow::StopIfNotOk(arrow::RegisterCancellingSignalHandler({SIGINT}));
  } else if (!enabled && was_enabled) {
    // disable override
    signal(SIGINT, previous_signal_handler_);
    previous_signal_handler_ = nullptr;
    arrow::UnregisterCancellingSignalHandler();
    arrow::ResetSignalStopSource();
    stop_source_ = nullptr;
  }
}

// [[arrow::export]]
bool CanRunWithCapturedR() {
#if defined(HAS_UNWIND_PROTECT)
  return GetMainRThread().Executor() == nullptr;
#else
  return false;
#endif
}

// [[arrow::export]]
std::string TestSafeCallIntoR(cpp11::function r_fun_that_returns_a_string,
                              std::string opt) {
  if (opt == "async_with_executor") {
    std::thread* thread_ptr;

    auto result =
        RunWithCapturedR<std::string>([&thread_ptr, r_fun_that_returns_a_string]() {
          auto fut = arrow::Future<std::string>::Make();
          thread_ptr = new std::thread([fut, r_fun_that_returns_a_string]() mutable {
            auto result = SafeCallIntoR<std::string>([&] {
              return cpp11::as_cpp<std::string>(r_fun_that_returns_a_string());
            });

            fut.MarkFinished(result);
          });

          return fut;
        });

    thread_ptr->join();
    delete thread_ptr;

    return arrow::ValueOrStop(result);
  } else if (opt == "async_without_executor") {
    std::thread* thread_ptr;

    auto fut = arrow::Future<std::string>::Make();
    thread_ptr = new std::thread([fut, r_fun_that_returns_a_string]() mutable {
      auto result = SafeCallIntoR<std::string>(
          [&] { return cpp11::as_cpp<std::string>(r_fun_that_returns_a_string()); });

      if (result.ok()) {
        fut.MarkFinished(result.ValueUnsafe());
      } else {
        fut.MarkFinished(result.status());
      }
    });

    thread_ptr->join();
    delete thread_ptr;

    // We should be able to get this far, but fut will contain an error
    // because it tried to evaluate R code from another thread
    return arrow::ValueOrStop(fut.result());

  } else if (opt == "on_main_thread") {
    auto result = SafeCallIntoR<std::string>(
        [&]() { return cpp11::as_cpp<std::string>(r_fun_that_returns_a_string()); });
    arrow::StopIfNotOk(result.status());
    return result.ValueUnsafe();
  } else {
    cpp11::stop("Unknown `opt`");
  }
}
