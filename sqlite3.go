package sqlite3

import (
	"github.com/riposo/riposo/pkg/plugin"

	_ "github.com/riposo/sqlite3/internal/cache"
	_ "github.com/riposo/sqlite3/internal/permission"
	_ "github.com/riposo/sqlite3/internal/storage"
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
