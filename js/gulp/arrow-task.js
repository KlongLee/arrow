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

const {
    targetDir, observableFromStreams
} = require('./util');

const del = require('del');
const gulp = require('gulp');
const mkdirp = require('mkdirp');
const gulpRename = require(`gulp-rename`);
const { memoizeTask } = require('./memoize-task');
const { Observable, ReplaySubject } = require('rxjs');
const pipeline = require('util').promisify(require('stream').pipeline);

const arrowTask = ((cache) => memoizeTask(cache, function copyMain(target) {
    const out = targetDir(target);
    const dtsGlob = `${targetDir(`esnext`, `cjs`)}/**/*.ts`;
    const cjsGlob = `${targetDir(`es2015`, `cjs`)}/**/*.js`;
    const esmGlob = `${targetDir(`es2015`, `esm`)}/**/*.js`;
    const es2015UmdGlob = `${targetDir(`es2015`, `umd`)}/*.js`;
    const esnextUmdGlob = `${targetDir(`esnext`, `umd`)}/*.js`;
    const esmSourceMapsGlob = `${targetDir(`es2015`, `esm`)}/**/*.map`;
    const cjsSourceMapsGlob = `${targetDir(`es2015`, `cjs`)}/**/*.map`;
    const es2015UmdSourceMapsGlob = `${targetDir(`es2015`, `umd`)}/*.map`;
    const esnextUmdSourceMapsGlob = `${targetDir(`esnext`, `umd`)}/*.map`;
    return Observable.forkJoin(
        observableFromStreams(gulp.src(dtsGlob),                 gulp.dest(out)), // copy d.ts files
        observableFromStreams(gulp.src(esmGlob),                 gulp.dest(out)), // copy es2015 esm files
        observableFromStreams(gulp.src(esmSourceMapsGlob),       gulp.dest(out)), // copy es2015 esm sourcemaps
        observableFromStreams(gulp.src(cjsSourceMapsGlob),       gulp.dest(out)), // copy es2015 cjs sourcemaps
        observableFromStreams(gulp.src(es2015UmdSourceMapsGlob), gulp.dest(out)), // copy es2015 umd sourcemap files, but don't rename
        observableFromStreams(gulp.src(esnextUmdSourceMapsGlob), gulp.dest(out)), // copy esnext umd sourcemap files, but don't rename
        observableFromStreams(gulp.src(cjsGlob),       gulpRename((p) => { p.extname = '.cjs'; }),          gulp.dest(out)), // copy es2015 cjs files and rename to `.cjs`
        observableFromStreams(gulp.src(es2015UmdGlob), gulpRename((p) => { p.basename += `.es2015.min`; }), gulp.dest(out)), // copy es2015 umd files and add `.min`
        observableFromStreams(gulp.src(esnextUmdGlob), gulpRename((p) => { p.basename += `.esnext.min`; }), gulp.dest(out)), // copy esnext umd files and add `.esnext.min`
    ).publish(new ReplaySubject()).refCount();
}))({});

const arrowTSTask = ((cache) => memoizeTask(cache, async function copyTS(target, format) {
    const out = targetDir(target, format);
    await mkdirp(out);
    await pipeline(gulp.src(`src/**/*`), gulp.dest(out));
    await del(`${out}/**/*.js`);
}))({});


module.exports = arrowTask;
module.exports.arrowTask = arrowTask;
module.exports.arrowTSTask = arrowTSTask;
