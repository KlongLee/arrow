﻿using System;
using Grpc.Core;
using System.Threading.Tasks;

namespace Apache.Arrow.Flight.Client
{
    public class FlightRecordBatchExchangeCall : IDisposable
    {
        private readonly Func<Status> _getStatusFunc;
        private readonly Func<Metadata> _getTrailersFunc;
        private readonly Action _disposeAction;

        internal FlightRecordBatchExchangeCall(
            FlightClientRecordBatchStreamWriter requestStream,
            FlightClientRecordBatchStreamReader responseStream,
            Task<Metadata> responseHeadersAsync,
            Func<Status> getStatusFunc,
            Func<Metadata> getTrailersFunc,
            Action disposeAction)
        {
            RequestStream = requestStream;
            ResponseStream = responseStream;
            ResponseHeadersAsync = responseHeadersAsync;
            _getStatusFunc = getStatusFunc;
            _getTrailersFunc = getTrailersFunc;
            _disposeAction = disposeAction;
        }

        /// <summary>
        ///  Async stream to read streaming responses.
        /// </summary>
        public FlightClientRecordBatchStreamReader ResponseStream { get; }

        /// <summary>
        /// Async stream to send streaming requests.
        /// </summary>
        public FlightClientRecordBatchStreamWriter RequestStream { get; }

        /// <summary>
        /// Asynchronous access to response headers.
        /// </summary>
        public Task<Metadata> ResponseHeadersAsync { get; }

        /// <summary>
        /// Provides means to cleanup after the call. If the call has already finished normally
        /// (response stream has been fully read), doesn't do anything. Otherwise, requests
        /// cancellation of the call which should terminate all pending async operations
        /// associated with the call. As a result, all resources being used by the call should
        /// be released eventually.
        /// </summary>
        /// <remarks>
        /// Normally, there is no need for you to dispose the call unless you want to utilize
        /// the "Cancel" semantics of invoking Dispose.
        /// </remarks>
        public void Dispose()
        {
            _disposeAction();
        }

        /// <summary>
        /// Gets the call status if the call has already finished. Throws InvalidOperationException otherwise.
        /// </summary>
        /// <returns></returns>
        public Status GetStatus()
        {
            return _getStatusFunc();
        }

        /// <summary>
        /// Gets the call trailing metadata if the call has already finished. Throws InvalidOperationException otherwise.
        /// </summary>
        /// <returns></returns>
        public Metadata GetTrailers()
        {
            return _getTrailersFunc();
        }
    }
}
