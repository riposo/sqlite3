package sqlite3

import (
	"github.com/riposo/riposo/pkg/plugin"

	_ "github.com/riposo/sqlite3/internal/cache"      // auto-register cache adapter
	_ "github.com/riposo/sqlite3/internal/permission" // auto-register permission adapter
	_ "github.com/riposo/sqlite3/internal/storage"    // auto-register storage adapter
)

func init() {
	plugin.Register(plugin.New(
		"sqlite3",
		map[string]interface{}{
			"description": "The sqlite3 driver for Riposo storage, permissions and cache backends.",
			"url":         "https://github.com/riposo/sqlite3",
		},
		nil,
		nil,
	))
}
