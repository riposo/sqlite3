package sqlite3

import (
	"github.com/riposo/riposo/pkg/api"
	"github.com/riposo/riposo/pkg/plugin"

	_ "github.com/riposo/sqlite3/internal/cache"
	_ "github.com/riposo/sqlite3/internal/permission"
	_ "github.com/riposo/sqlite3/internal/storage"
)

func init() {
	plugin.Register("sqlite3", func(rts *api.Routes) (plugin.Plugin, error) {
		return pin{
			"description": "The sqlite3 driver for Riposo storage, permissions and cache backends.",
			"url":         "https://github.com/riposo/sqlite3",
		}, nil
	})
}

type pin map[string]interface{}

func (p pin) Meta() map[string]interface{} { return map[string]interface{}(p) }
func (pin) Close() error                   { return nil }
