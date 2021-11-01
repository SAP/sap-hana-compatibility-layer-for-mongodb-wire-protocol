// Copyright 2021 Baltoro OÜ.
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

package shared

import "github.com/MangoDB-io/MangoDB/internal/pgconn"

type Handler struct {
	pgPool   *pgconn.Pool
	peerAddr string
}

func NewHandler(pgPool *pgconn.Pool, peerAddr string) *Handler {
	return &Handler{
		pgPool:   pgPool,
		peerAddr: peerAddr,
	}
}
