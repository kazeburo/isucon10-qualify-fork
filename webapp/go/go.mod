module github.com/isucon/isucon10-qualify/isuumo

go 1.14

require (
	cloud.google.com/go v0.69.1
	contrib.go.opencensus.io/exporter/stackdriver v0.13.4
	contrib.go.opencensus.io/integrations/ocsql v0.1.6
	github.com/andybalholm/brotli v1.0.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/goccy/go-json v0.1.13
	github.com/gofiber/fiber/v2 v2.1.2
	github.com/jmoiron/sqlx v1.2.0
	github.com/klauspost/compress v1.11.2 // indirect
	github.com/stretchr/testify v1.5.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0
	go.opencensus.io v0.22.5
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/sys v0.0.0-20201101102859-da207088b7d1 // indirect
	gopkg.in/yaml.v2 v2.3.0 // indirect
)

replace github.com/goccy/go-json => github.com/kazeburo/go-json v0.1.14-0.20201105082108-a6ca3e9f316d
