#!/usr/bin/env python3
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

import os
from pathlib import Path
import subprocess

from dotenv import dotenv_values
from ruamel.yaml import YAML

from .utils.command import Command, default_bin


def flatten(node, parents=None):
    parents = list(parents or [])
    if isinstance(node, str):
        yield (node, parents)
    elif isinstance(node, list):
        for value in node:
            yield from flatten(value, parents=parents)
    elif isinstance(node, dict):
        for key, value in node.items():
            yield (key, parents)
            yield from flatten(value, parents=parents + [key])
    else:
        raise TypeError(node)


class UndefinedImage(Exception):
    pass


class Docker(Command):

    def __init__(self, docker_bin=None):
        self.bin = default_bin(docker_bin, "docker")


class DockerCompose(Command):

    def __init__(self, config_path, dotenv_path=None, compose_bin=None):
        self.config_path = Path(config_path)
        if dotenv_path:
            self.dotenv_path = Path(dotenv_path)
        else:
            self.dotenv_path = self.config_path.parent / '.env'

        yaml = YAML()
        with self.config_path.open() as fp:
            self.config = yaml.load(fp)

        self.nodes = dict(flatten(self.config['x-hierarchy']))
        self.dotenv = dotenv_values(self.dotenv_path)
        self.bin = default_bin(compose_bin, "docker-compose")

    def validate(self):
        services = self.config['services'].keys()
        nodes = self.nodes.keys()
        errors = []

        for name in nodes - services:
            errors.append(
                'Service `{}` is defined in `x-hierarchy` bot not in '
                '`services`'.format(name)
            )
        for name in services - nodes:
            errors.append(
                'Service `{}` is defined in `services` but not in '
                '`x-hierarchy`'.format(name)
            )

        # trigger docker-compose's own validation
        result = self._execute('ps', check=False, stderr=subprocess.PIPE,
                               stdout=subprocess.PIPE)

        if result.returncode != 0:
            # strip the intro line of docker-compose errors
            errors += result.stderr.decode().splitlines()[1:]

        if errors:
            msg = '\n'.join([' - {}'.format(msg) for msg in errors])
            raise ValueError(
                'Found errors with docker-compose:\n{}'.format(msg)
            )

    def _compose_env(self):
        return dict(os.environ, **self.dotenv)

    def _validate_image(self, name):
        if name not in self.nodes:
            raise UndefinedImage(name)

    def _execute(self, *args, **kwargs):
        # set default arguments for docker-compose
        return super().run('--file', str(self.config_path), *args, **kwargs)

    def build(self, image, cache=True, cache_leaf=True):
        self._validate_image(image)
        env = self._compose_env()

        # build all parents
        for parent in self.nodes[image]:
            if cache:
                self._execute('pull', '--ignore-pull-failures', parent,
                              env=env)
                self._execute('build', parent, env=env)
            else:
                self._execute('build', '--no-cache', parent, env=env)

        # build the image at last
        if cache and cache_leaf:
            self._execute('pull', '--ignore-pull-failures', image, env=env)
            self._execute('build', image, env=env)
        else:
            self._execute('build', '--no-cache', image, env=env)

    def run(self, image, command=None, env=None):
        self._validate_image(image)
        args = []
        if env is not None:
            for k, v in env.items():
                args.extend(['-e', '{}={}'.format(k, v)])
        args.append(image)
        if command is not None:
            args.append(command)
        self._execute('run', '--rm', *args, env=self._compose_env())

    def push(self, image, user, password):
        try:
            Docker().run('login', '-u', user, '-p', password)
        except subprocess.CalledProcessError:
            # hide credentials
            msg = ('Failed to push `{}`, check the passed credentials'
                   .format(image))
            raise RuntimeError(msg) from None
        else:
            self._execute('push', image, env=self._compose_env())
