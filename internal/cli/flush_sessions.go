// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cli // import "miniflux.app/v2/internal/cli"

import (
	"fmt"

	"miniflux.app/v2/internal/storage"
)

func flushSessions(store *storage.Storage) {
	fmt.Println("Flushing all sessions (disconnect users)")

	generation, err := store.FlushAllSessions()
	if err != nil {
		printErrorAndExit(err)
	}

	fmt.Printf("All web sessions revoked; new session generation is %d.\n", generation)
}
