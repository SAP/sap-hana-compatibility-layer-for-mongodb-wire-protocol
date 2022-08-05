// SPDX-FileCopyrightText: 2021 FerretDB Inc.
//
// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

// Copyright 2021 FerretDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fjson

import (
	"testing"
	"time"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
)

func convertDocument(d types.Document) *Document {
	res := Document(d)
	return &res
}

var (
	handshake1 = testCase{
		name: "handshake1",
		v: convertDocument(types.MustMakeDocument(
			"ismaster", true,
			"client", types.MustMakeDocument(
				"driver", types.MustMakeDocument(
					"name", "nodejs",
					"version", "4.0.0-beta.6",
				),
				"os", types.MustMakeDocument(
					"type", "Darwin",
					"name", "darwin",
					"architecture", "x64",
					"version", "20.6.0",
				),
				"platform", "Node.js v14.17.3, LE (unified)|Node.js v14.17.3, LE (unified)",
				"application", types.MustMakeDocument(
					"name", "mongosh 1.0.1",
				),
			),
			"compression", types.MustNewArray("none"),
			"loadBalanced", false,
		)),
		j: `{"ismaster":true,"client":{"driver":{"name":"nodejs","version":"4.0.0-beta.6"},` +
			`"os":{"type":"Darwin","name":"darwin","architecture":"x64","version":"20.6.0"},` +
			`"platform":"Node.js v14.17.3, LE (unified)|Node.js v14.17.3, LE (unified)",` +
			`"application":{"name":"mongosh 1.0.1"}},"compression":["none"],"loadBalanced":false}`,
	}

	handshake2 = testCase{
		name: "handshake2",
		v: convertDocument(types.MustMakeDocument(
			"ismaster", true,
			"client", types.MustMakeDocument(
				"driver", types.MustMakeDocument(
					"name", "nodejs",
					"version", "4.0.0-beta.6",
				),
				"os", types.MustMakeDocument(
					"type", "Darwin",
					"name", "darwin",
					"architecture", "x64",
					"version", "20.6.0",
				),
				"platform", "Node.js v14.17.3, LE (unified)|Node.js v14.17.3, LE (unified)",
				"application", types.MustMakeDocument(
					"name", "mongosh 1.0.1",
				),
			),
			"compression", types.MustNewArray("none"),
			"loadBalanced", false,
		)),
		j: `{"ismaster":true,` +
			`"client":{"driver":{` +
			`"name":"nodejs","version":"4.0.0-beta.6"},"os":{` +
			`"type":"Darwin","name":"darwin","architecture":"x64","version":"20.6.0"},` +
			`"platform":"Node.js v14.17.3, LE (unified)|Node.js v14.17.3, LE (unified)",` +
			`"application":{"name":"mongosh 1.0.1"}},"compression":["none"],"loadBalanced":false}`,
	}

	// handshake3 = testCase{
	// 	name: "handshake3",
	// 	v: convertDocument(types.MustMakeDocument(
	// 		"buildInfo", int32(1),
	// 		"lsid", types.MustMakeDocument(
	// 			"id", types.Binary{
	// 				Subtype: types.BinaryUUID,
	// 				B:       []byte{0xa3, 0x19, 0xf2, 0xb4, 0xa1, 0x75, 0x40, 0xc7, 0xb8, 0xe7, 0xa3, 0xa3, 0x2e, 0xc2, 0x56, 0xbe},
	// 			},
	// 		),
	// 		"$db", "admin",
	// 	)),
	// 	j: `{"$k":["buildInfo","lsid","$db"],"buildInfo":1,` +
	// 		`"lsid":{"$k":["id"],"id":{"$b":"oxnytKF1QMe456OjLsJWvg==","s":4}},"$db":"admin"}`,
	// }

	handshake4 = testCase{
		name: "handshake4",
		v: convertDocument(types.MustMakeDocument(
			"version", "5.0.0",
			"gitVersion", "1184f004a99660de6f5e745573419bda8a28c0e9",
			"modules", types.MustNewArray(),
			"allocator", "tcmalloc",
			"javascriptEngine", "mozjs",
			"sysInfo", "deprecated",
			"versionArray", types.MustNewArray(int32(5), int32(0), int32(0), int32(0)),
			"openssl", types.MustMakeDocument(
				"running", "OpenSSL 1.1.1f  31 Mar 2020",
				"compiled", "OpenSSL 1.1.1f  31 Mar 2020",
			),
			"buildEnvironment", types.MustMakeDocument(
				"distmod", "ubuntu2004",
				"distarch", "x86_64",
				"cc", "/opt/mongodbtoolchain/v3/bin/gcc: gcc (GCC) 8.5.0",
				"ccflags", "-Werror -include mongo/platform/basic.h -fasynchronous-unwind-tables -ggdb "+
					"-Wall -Wsign-compare -Wno-unknown-pragmas -Winvalid-pch -fno-omit-frame-pointer "+
					"-fno-strict-aliasing -O2 -march=sandybridge -mtune=generic -mprefer-vector-width=128 "+
					"-Wno-unused-local-typedefs -Wno-unused-function -Wno-deprecated-declarations "+
					"-Wno-unused-const-variable -Wno-unused-but-set-variable -Wno-missing-braces "+
					"-fstack-protector-strong -Wa,--nocompress-debug-sections -fno-builtin-memcmp",
				"cxx", "/opt/mongodbtoolchain/v3/bin/g++: g++ (GCC) 8.5.0",
				"cxxflags", "-Woverloaded-virtual -Wno-maybe-uninitialized -fsized-deallocation -std=c++17",
				"linkflags", "-Wl,--fatal-warnings -pthread -Wl,-z,now -fuse-ld=gold -fstack-protector-strong "+
					"-Wl,--no-threads -Wl,--build-id -Wl,--hash-style=gnu -Wl,-z,noexecstack -Wl,--warn-execstack "+
					"-Wl,-z,relro -Wl,--compress-debug-sections=none -Wl,-z,origin -Wl,--enable-new-dtags",
				"target_arch", "x86_64",
				"target_os", "linux",
				"cppdefines", "SAFEINT_USE_INTRINSICS 0 PCRE_STATIC NDEBUG _XOPEN_SOURCE 700 _GNU_SOURCE "+
					"_REENTRANT 1 _FORTIFY_SOURCE 2 BOOST_THREAD_VERSION 5 BOOST_THREAD_USES_DATETIME "+
					"BOOST_SYSTEM_NO_DEPRECATED BOOST_MATH_NO_LONG_DOUBLE_MATH_FUNCTIONS "+
					"BOOST_ENABLE_ASSERT_DEBUG_HANDLER BOOST_LOG_NO_SHORTHAND_NAMES BOOST_LOG_USE_NATIVE_SYSLOG "+
					"BOOST_LOG_WITHOUT_THREAD_ATTR ABSL_FORCE_ALIGNED_ACCESS",
			),
			"bits", int32(64),
			"debug", false,
			"maxBsonObjectSize", int32(16777216),
			"storageEngines", types.MustNewArray("devnull", "ephemeralForTest", "wiredTiger"),
			"ok", int32(1),
		)),
		j: `{"version":"5.0.0","gitVersion":"1184f004a99660de6f5e745573419bda8a28c0e9","modules":[],` +
			`"allocator":"tcmalloc","javascriptEngine":"mozjs","sysInfo":"deprecated","versionArray":[5,0,0,0],` +
			`"openssl":{"running":"OpenSSL 1.1.1f  31 Mar 2020",` +
			`"compiled":"OpenSSL 1.1.1f  31 Mar 2020"},` +
			`"buildEnvironment":{"distmod":"ubuntu2004","distarch":"x86_64",` +
			`"cc":"/opt/mongodbtoolchain/v3/bin/gcc: gcc (GCC) 8.5.0",` +
			`"ccflags":"-Werror -include mongo/platform/basic.h -fasynchronous-unwind-tables -ggdb -Wall ` +
			`-Wsign-compare -Wno-unknown-pragmas -Winvalid-pch -fno-omit-frame-pointer -fno-strict-aliasing ` +
			`-O2 -march=sandybridge -mtune=generic -mprefer-vector-width=128 -Wno-unused-local-typedefs ` +
			`-Wno-unused-function -Wno-deprecated-declarations -Wno-unused-const-variable ` +
			`-Wno-unused-but-set-variable -Wno-missing-braces -fstack-protector-strong ` +
			`-Wa,--nocompress-debug-sections -fno-builtin-memcmp",` +
			`"cxx":"/opt/mongodbtoolchain/v3/bin/g++: g++ (GCC) 8.5.0",` +
			`"cxxflags":"-Woverloaded-virtual -Wno-maybe-uninitialized -fsized-deallocation -std=c++17",` +
			`"linkflags":"-Wl,--fatal-warnings -pthread -Wl,-z,now -fuse-ld=gold -fstack-protector-strong ` +
			`-Wl,--no-threads -Wl,--build-id -Wl,--hash-style=gnu -Wl,-z,noexecstack -Wl,--warn-execstack ` +
			`-Wl,-z,relro -Wl,--compress-debug-sections=none -Wl,-z,origin -Wl,--enable-new-dtags",` +
			`"target_arch":"x86_64","target_os":"linux",` +
			`"cppdefines":"SAFEINT_USE_INTRINSICS 0 PCRE_STATIC NDEBUG _XOPEN_SOURCE 700 _GNU_SOURCE ` +
			`_REENTRANT 1 _FORTIFY_SOURCE 2 BOOST_THREAD_VERSION 5 BOOST_THREAD_USES_DATETIME ` +
			`BOOST_SYSTEM_NO_DEPRECATED BOOST_MATH_NO_LONG_DOUBLE_MATH_FUNCTIONS BOOST_ENABLE_ASSERT_DEBUG_HANDLER ` +
			`BOOST_LOG_NO_SHORTHAND_NAMES BOOST_LOG_USE_NATIVE_SYSLOG BOOST_LOG_WITHOUT_THREAD_ATTR ` +
			`ABSL_FORCE_ALIGNED_ACCESS"},"bits":64,"debug":false,"maxBsonObjectSize":16777216,` +
			`"storageEngines":["devnull","ephemeralForTest","wiredTiger"],"ok":1}`,
	}

	all = testCase{
		name: "all",
		v: convertDocument(types.MustMakeDocument(
			"bool", types.MustNewArray(true, false),
			"datetime", types.MustNewArray(time.Date(2021, 7, 27, 9, 35, 42, 123000000, time.UTC).Local(), time.Time{}.Local()),
			"double", types.MustNewArray(42.13),
			"int32", types.MustNewArray(int32(42), int32(0)),
			"int64", types.MustNewArray(int64(223372036854775807)),
			"objectID", types.MustNewArray(types.ObjectID{0x42}, types.ObjectID{}),
			"string", types.MustNewArray("foo", ""),
		)),
		j: `{"bool":[true,false],` +
			`"datetime":[{"$da":1627378542123},{"$da":-62135596800000}],"double":[42.13],` +
			`"int32":[42,0],"int64":[223372036854775807],` +
			`"objectID":[{"oid":"420000000000000000000000"},{"oid":"000000000000000000000000"}],` +
			`"string":["foo",""]}`,
	}

	eof = testCase{
		name: "EOF",
		j:    `[`,
		jErr: `unexpected EOF`,
	}

	documentTestCases = []testCase{handshake1, handshake2, handshake4, all, eof}
)

func TestDocument(t *testing.T) {
	t.Parallel()
	testJSON(t, documentTestCases, func() fjsontype { return new(Document) })
}

func FuzzDocument(f *testing.F) {
	fuzzJSON(f, documentTestCases, func() fjsontype { return new(Document) })
}

func BenchmarkDocument(b *testing.B) {
	benchmark(b, documentTestCases, func() fjsontype { return new(Document) })
}
