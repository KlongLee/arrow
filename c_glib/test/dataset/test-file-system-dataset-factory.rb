# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

class TestDatasetFileSystemDatasetFactory < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Writable

  def setup
    omit("Arrow Dataset is required") unless defined?(ArrowDataset)
    Dir.mktmpdir do |tmpdir|
      @dir = tmpdir
      @path = File.join(@dir, "table.arrow")
      @table = build_table(visible: [
                             build_boolean_array([true, false, true]),
                             build_boolean_array([false, true, false, true]),
                           ],
                           point: [
                             build_int32_array([1, 2, 3]),
                             build_int32_array([-1, -2, -3, -4]),
                           ])
      @format = ArrowDataset::IPCFileFormat.new
      write_table(@table, @path)
      yield
    end
  end

  def test_file_system
    factory = ArrowDataset::FileSystemDatasetFactory.new(@format)
    factory.file_system = Arrow::LocalFileSystem.new
    factory.add_path(File.expand_path(@path))
    dataset = factory.finish
    assert_equal(@table, dataset.to_table)
  end

  def test_file_system_uri
    factory = ArrowDataset::FileSystemDatasetFactory.new(@format)
    factory.file_system_uri = "file://#{File.expand_path(@path)}"
    dataset = factory.finish
    assert_equal(@table, dataset.to_table)
  end
end
