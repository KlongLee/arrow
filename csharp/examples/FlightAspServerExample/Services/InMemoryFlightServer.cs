using Apache.Arrow;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Server;
using Grpc.Core;

namespace FlightAspServerExample.Services
{
    public class FlightData
    {
        public FlightData()
        {
            flights = new Dictionary<FlightTicket, FlightInfo> { };
            tables = new Dictionary<FlightTicket, List<RecordBatch>> { };
        }
        public IDictionary<FlightTicket, FlightInfo> flights;
        public IDictionary<FlightTicket, List<RecordBatch>> tables;
    }

    public class InMemoryFlightServer : FlightServer
    {
        private FlightData _flightData;

        public InMemoryFlightServer(FlightData flightData)
        {
            _flightData = flightData;
        }

        public override async Task DoPut(
            FlightServerRecordBatchStreamReader requestStream,
            IAsyncStreamWriter<FlightPutResult> responseStream,
            ServerCallContext context
        )
        {
            var newTable = new List<RecordBatch> { };
            Int64 numRows = 0;

            await foreach (var batch in requestStream.ReadAllAsync(context.CancellationToken))
            {
                newTable.Add(batch);
                numRows += batch.Length;
            }

            var descriptor = await requestStream.FlightDescriptor;
            var ticket = DescriptorAsTicket(descriptor);
            var schema = await requestStream.Schema;

            _flightData.flights.Add(ticket, new FlightInfo(
                schema,
                descriptor,
                new List<FlightEndpoint> { GetEndpoint(ticket, $"http://{context.Host}") },
                numRows,
                -1 // Unknown
            ));
            _flightData.tables.Add(ticket, newTable);

            await responseStream.WriteAsync(new FlightPutResult("Table saved."));
        }

        public override async Task DoGet(
            FlightTicket ticket,
            FlightServerRecordBatchStreamWriter responseStream,
            ServerCallContext context
        )
        {
            if (!_flightData.tables.ContainsKey(ticket))
            {
                throw new RpcException(new Status(StatusCode.NotFound, "Flight not found."));
            }
            var table = _flightData.tables[ticket];

            foreach (var batch in table) {
                await responseStream.WriteAsync(batch);
            }
        }

        public override async Task ListFlights(
            FlightCriteria request,
            IAsyncStreamWriter<FlightInfo> responseStream,
            ServerCallContext context
        )
        {
            foreach (var flight in _flightData.flights.Values)
            {
                await responseStream.WriteAsync(flight);
            }
        }

        public override async Task<FlightInfo> GetFlightInfo(FlightDescriptor request, ServerCallContext context)
        {
            var key = DescriptorAsTicket(request);
            if (_flightData.flights.ContainsKey(key))
            {
                return _flightData.flights[key];
            }
            else
            {
                throw new RpcException(new Status(StatusCode.NotFound, "Flight not found."));
            }
        }

        public override async Task ListActions(
            IAsyncStreamWriter<FlightActionType> responseStream, 
            ServerCallContext context
        )
        {
            await responseStream.WriteAsync(new FlightActionType("clear", "Clear the flights from the server"));
        }

        public override async Task DoAction(
            FlightAction request, 
            IAsyncStreamWriter<FlightResult> responseStream, 
            ServerCallContext context
        )
        {
            if (request.Type == "clear")
            {
                _flightData.flights.Clear();
                _flightData.tables.Clear();
            }
            else
            {
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Action does not exist."));
            }
        }

        public override async Task<Schema> GetSchema(FlightDescriptor request, ServerCallContext context)
        {
            var info = await GetFlightInfo(request, context);
            return info.Schema;
        }

        private FlightTicket DescriptorAsTicket(FlightDescriptor desc)
        {
            return new FlightTicket(desc.ToString());
        }

        private FlightEndpoint GetEndpoint(FlightTicket ticket, string host)
        {
            var location = new FlightLocation(host);
            return new FlightEndpoint(ticket, new List<FlightLocation> { location });
        }
    }
}