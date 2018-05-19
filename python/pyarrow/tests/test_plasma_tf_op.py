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

import numpy as np
import pytest


@pytest.mark.plasma
def test_plasma_tf_op(use_gpu=False):
    import pyarrow.plasma as plasma

    if not plasma.has_tf_plasma_op:
        return

    try:
        import tensorflow as tf
    except ImportError:
        pytest.skip("TensorFlow not installed")

    with plasma.start_plasma_store(10**8) as (plasma_store_name, p):
        FORCE_DEVICE = '/gpu' if use_gpu else '/cpu'

        object_id = np.random.bytes(20)

        data = np.random.randn(3, 244, 244).astype(np.float32)
        kOnes = np.ones((3, 244, 244)).astype(np.float32)

        sess = tf.Session(config=tf.ConfigProto(
            allow_soft_placement=True, log_device_placement=True))

        def ToPlasma():
            data_t = tf.constant(data)
            data_t = tf.Print(data_t, [data_t], "data_t = ")
            kOnes_t = tf.constant(kOnes)
            kOnes_t = tf.Print(kOnes_t, [kOnes_t], "kOnes_t = ")
            return plasma.tf_plasma_op.tensor_to_plasma(
                [data_t, kOnes_t],
                object_id,
                plasma_store_socket_name=plasma_store_name,
                plasma_manager_socket_name="")

        def FromPlasma():
            return plasma.tf_plasma_op.plasma_to_tensor(
                object_id,
                plasma_store_socket_name=plasma_store_name,
                plasma_manager_socket_name="")

        with tf.device(FORCE_DEVICE):
            to_plasma = ToPlasma()
            from_plasma = FromPlasma()

        z = from_plasma + 1

        sess.run(to_plasma)
        print('Getting object...')
        # NOTE(zongheng): currently it returns a flat 1D tensor.
        # So reshape manually.
        out = sess.run(from_plasma)
        # print('out.shape: %s' % out.shape)
        # print('out: %s' % out)

        out = np.split(out, 2)
        out0 = out[0].reshape(3, 244, 244)
        out1 = out[1].reshape(3, 244, 244)

        # print('data: %s' % data)
        # print('out0: %s' % out0)
        # print('out1: %s' % out1)

        sess.run(z)

        assert np.array_equal(data, out0), "Data not equal!"
        assert np.array_equal(kOnes, out1), "Data not equal!"
        print('OK: data all equal')
