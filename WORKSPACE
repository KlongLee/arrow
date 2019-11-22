load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "double-conversion",
    build_file = "//bazel:double_conversion.BUILD",
    sha256 = "a63ecb93182134ba4293fd5f22d6e08ca417caafa244afaa751cbfddf6415b13",
    strip_prefix = "double-conversion-3.1.5",
    urls = ["https://github.com/google/double-conversion/archive/v3.1.5.tar.gz"],
)

http_archive(
    name = "googletest",
    sha256 = "9dc9157a9a1551ec7a7e43daea9a694a0bb5fb8bec81235d8a1e6ef64c716dcb",
    strip_prefix = "googletest-release-1.10.0",
    urls = ["https://github.com/google/googletest/archive/release-1.10.0.tar.gz"],
)

http_archive(
    name = "com_google_protobuf",
    strip_prefix = "protobuf-3.9.1",
    urls = ["https://github.com/google/protobuf/archive/v3.9.1.tar.gz"],
)

http_archive(
    name = "build_bazel_rules_apple",
    sha256 = "53a8f9590b4026fbcfefd02c868e48683b44a314338d03debfb8e8f6c50d1239",
    strip_prefix = "rules_apple-0.18.0",
    urls = ["https://github.com/bazelbuild/rules_apple/archive/0.18.0.tar.gz"],
)

http_archive(
    name = "com_github_grpc_grpc",
    strip_prefix = "grpc-b74d7e4d14408fc1ade4975271aa05eb99441720",
    urls = ["https://github.com/grpc/grpc/archive/b74d7e4d14408fc1ade4975271aa05eb99441720.tar.gz"],
)

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()

http_archive(
    name = "rapidjson",
    build_file = "//bazel:rapidjson.BUILD",
    sha256 = "de623a7577defec15b55f82813a05a6f0fe60e337ffa8a5ee4b2c13bd8417028",
    strip_prefix = "rapidjson-2bbd33b33217ff4a73434ebf10cdac41e2ef5e34/include",
    urls = ["https://github.com/miloyip/rapidjson/archive/2bbd33b33217ff4a73434ebf10cdac41e2ef5e34.tar.gz"],
)

new_local_repository(
    name = "flatbuffers",
    build_file = "//bazel:flatbuffers.BUILD",
    path = "cpp/thirdparty/flatbuffers/include",
)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_foreign_cc",
    sha256 = "bdfc2734367a1242514251c7ed2dd12f65dd6d19a97e6a2c61106851be8e7fb8",
    strip_prefix = "rules_foreign_cc-master",
    url = "https://github.com/bazelbuild/rules_foreign_cc/archive/master.zip",
)

load("@rules_foreign_cc//:workspace_definitions.bzl", "rules_foreign_cc_dependencies")

rules_foreign_cc_dependencies()

http_archive(
    name = "boost",
    build_file = "//bazel:boost.BUILD",
    sha256 = "da3411ea45622579d419bfda66f45cd0f8c32a181d84adfa936f5688388995cf",
    strip_prefix = "boost_1_68_0",
    urls = ["https://dl.bintray.com/boostorg/release/1.68.0/source/boost_1_68_0.tar.gz"],
)

new_local_repository(
    name = "numpy",
    build_file = "//bazel:numpy.BUILD",
    path = "/usr"
    #strip_prefix = "numpy-1.17.3",
    #urls = ["https://github.com/numpy/numpy/archive/v1.17.3.tar.gz"],
)

new_local_repository(
    name = "python",
    build_file = "//bazel:python.BUILD",
    path = "/usr",
)

http_archive(
        name = "cython",
        strip_prefix = "cython-0.29.14",
        urls = [
            "https://github.com/cython/cython/archive/0.29.14.tar.gz",
        ],
        build_file = "//bazel:cython.BUILD",
    )
