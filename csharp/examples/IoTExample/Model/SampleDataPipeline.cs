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

using System;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;
using System.Threading.Channels;
using System.Threading;
using System.Collections.Concurrent;

namespace IoTPipelineExample
{
    public class SampleDataPipeline
    {
        private int _size;
        private int _partitions;
        private readonly int _inputs;
        private readonly int _capacity;
        private readonly Channel<SensorData> _channel;
        ChannelWriter<SensorData> _writer;
        ChannelReader<SensorData> _reader;

        private readonly List<ConcurrentBag<int>> _colSubjectIdArrays;
        private readonly List<ConcurrentBag<string>> _colActivityLabelArrays;
        private readonly List<ConcurrentBag<long>> _colTimestampArrays;
        private readonly List<ConcurrentBag<double>> _colXAxisArrays;
        private readonly List<ConcurrentBag<double>> _colYAxisArrays;
        private readonly List<ConcurrentBag<double>> _colZAxisArrays;

        public Dictionary<string, string> activityLabel = new Dictionary<string, string>()
            {
                {"walking", "A"},
                {"jogging", "B"},
                {"stairs", "C"},
                {"sitting", "D"},
                {"standing", "E"},
                {"typing", "F"},
                {"teeth", "G"},
                {"soup", "H"},
                {"chips", "I"},
                {"pasta", "J"},
                {"drinking", "K"},
                {"sandwich", "L"},
                {"kicking", "M"},
                {"catch", "O"},
                {"dribbling", "P"},
                {"writing", "Q"},
                {"clapping", "R"},
                {"folding", "S"},
            };

        public SampleDataPipeline(int concurrencyLevel, int totalSensorData, int queueCapacity)
        {
            _partitions = concurrencyLevel;
            _inputs = totalSensorData;
            _capacity = queueCapacity;
            _channel = Channel.CreateBounded<SensorData>(_capacity);
            _writer = _channel.Writer;
            _reader = _channel.Reader;

            _colSubjectIdArrays = new List<ConcurrentBag<int>>();
            _colActivityLabelArrays = new List<ConcurrentBag<string>>();
            _colTimestampArrays = new List<ConcurrentBag<long>>();
            _colXAxisArrays = new List<ConcurrentBag<double>>();
            _colYAxisArrays = new List<ConcurrentBag<double>>();
            _colZAxisArrays = new List<ConcurrentBag<double>>();

            for (int i = 0; i < _partitions; i++)
            {
                _colSubjectIdArrays.Add(new ConcurrentBag<int>());
                _colActivityLabelArrays.Add(new ConcurrentBag<string>());
                _colTimestampArrays.Add(new ConcurrentBag<long>());
                _colXAxisArrays.Add(new ConcurrentBag<double>());
                _colYAxisArrays.Add(new ConcurrentBag<double>());
                _colZAxisArrays.Add(new ConcurrentBag<double>());
            }
        }

        public async Task WriteToChannel(int taskNumber)
        {
            Random rand = new Random();
            List<string> keyList = new List<string>(activityLabel.Keys);
            int count = keyList.Count;
            DateTime now = DateTime.Now;
            long unixTime = ((DateTimeOffset)now).ToUnixTimeSeconds();

            Console.WriteLine($"Write to channel task {taskNumber} started!");
            while (await _writer.WaitToWriteAsync())
            {
                string randomKey = keyList[rand.Next(count)];
                string label = activityLabel[randomKey];

                // generate random missing values
                if (rand.Next(10_000) == 9_999)
                {
                    label = null;
                }

                var item = new SensorData
                {
                    subjectId = rand.Next(1000, 2001),
                    activityLabel = label,
                    timestamp = unixTime++,
                    x_Axis = rand.NextDouble(),
                    y_Axis = rand.NextDouble(),
                    z_Axis = rand.NextDouble(),
                };

                if (_writer.TryWrite(item))
                {
                    Interlocked.Increment(ref _size);

                    if (_size >= _inputs)
                    {
                        _writer.TryComplete();
                    }
                }
            }

            Console.WriteLine($"Write to channel task {taskNumber} finished!");
        }

        public async Task ReadFromChannel(int taskNumber)
        {
            Console.WriteLine($"Read from channel task {taskNumber} started!");
            while (await _reader.WaitToReadAsync())
            {
                while (_reader.TryRead(out SensorData item))
                {
                    if (item != null && item.subjectId != null)
                    {
                        var hashKey = item.subjectId % _partitions;

                        _colSubjectIdArrays[(int)hashKey].Add((int)item.subjectId);
                        _colActivityLabelArrays[(int)hashKey].Add(item.activityLabel);
                        _colTimestampArrays[(int)hashKey].Add((long)item.timestamp);
                        _colXAxisArrays[(int)hashKey].Add((double)item.x_Axis);
                        _colYAxisArrays[(int)hashKey].Add((double)item.y_Axis);
                        _colZAxisArrays[(int)hashKey].Add((double)item.z_Axis);
                    }
                }
            }
            Console.WriteLine($"Read from channel task {taskNumber} finished!");
        }

        public async Task<string> PersistData()
        {
            int partitionNumber = 0;
            string currentPath = Directory.GetCurrentDirectory();
            string arrowDataPath = Path.Combine(currentPath, "arrow");
            if (!Directory.Exists(arrowDataPath))
                Directory.CreateDirectory(arrowDataPath);

            for (int i = 0; i < _colSubjectIdArrays.Count; i++)
            {
                var memoryAllocator = new NativeMemoryAllocator(alignment: 64);

                var recordBatch = new RecordBatch.Builder(memoryAllocator)
                    .Append("SubjectId", false, col => col.Int32(array => array.AppendRange(_colSubjectIdArrays[i])))
                    .Append("ActivityLabel", false, col => col.String(array => array.AppendRange(_colActivityLabelArrays[i])))
                    .Append("Timestamp", false, col => col.Int64(array => array.AppendRange(_colTimestampArrays[i])))
                    .Append("XAxis", false, col => col.Double(array => array.AppendRange(_colXAxisArrays[i])))
                    .Append("YAxis", false, col => col.Double(array => array.AppendRange(_colYAxisArrays[i])))
                    .Append("ZAxis", false, col => col.Double(array => array.AppendRange(_colZAxisArrays[i])))
                    .Build();

                using (var stream = File.OpenWrite(arrowDataPath + @"\iotbigdata_" + partitionNumber + ".arrow"))
                using (var writer = new ArrowFileWriter(stream, recordBatch.Schema))
                {
                    await writer.WriteRecordBatchAsync(recordBatch);
                    await writer.WriteEndAsync();
                }

                partitionNumber++;
            }

            return arrowDataPath;
        }
    }

}
