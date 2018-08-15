package main

import (
	"database/sql"
	"github.com/go-on/pj"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
	"net/http"
	"os"
)

// would come from the filesystem and be loaded on start/init
var queries = map[string]map[string]string{}

func printErr(err error, r *http.Request) {
	println(err.Error())
}

func addQuery(path string, meth string, queryFunc string) {
	queryMap, has := queries[path]
	if !has {
		queryMap = map[string]string{}
		queries[path] = queryMap
	}
	if _, exists := queryMap[meth]; exists {
		panic("query for " + meth + "path " + path + " already exists")
	}
	queryMap[meth] = queryFunc
}

func init() {
	addQuery("persons", "GET", "all_persons")
	// addQuery("person", "GET", "single_person")
	// addQuery("person", "POST", "add_person")
	// addQuery("person", "DELETE", "delete_person")
	// addQuery("person", "PATCH", "patch_person")
}

func run() (err error) {
	var (
		conf pgx.ConnConfig
		pool *pgx.ConnPool
		db   *sql.DB
	)

steps:
	for jump := 1; err == nil; jump++ {
		switch jump - 1 {
		default:
			break steps
		case 0:
			conf, err = pgx.ParseURI(os.Getenv("PG_URL"))
		case 1:
			pool, err = pgx.NewConnPool(pgx.ConnPoolConfig{ConnConfig: conf})
		case 2:
			db, err = stdlib.OpenFromConnPool(pool)
		case 3:
			for p, m := range queries {
				http.Handle("/"+p, pj.New(db, m, printErr))
			}
			http.ListenAndServe(":8080", nil)
		}
	}
	return
}

/*
The following SQL should be run inside your postgres installation

CREATE extension plv8;

CREATE OR REPLACE FUNCTION all_persons(params json) RETURNS text AS $function$
	if (typeof params != 'object')
		return NULL;
  var o = {};
  o["user"] = params["user"];
  o["results"] = [{"firstname": "peter", "lastname": "pan"}];
  o["http_status_code"] = 201;
  o["http_headers"] = {"last-mod": "now"};
  return JSON.stringify(o);
$function$ LANGUAGE plv8 IMMUTABLE STRICT;
*/

func main() {
	err := run()
	if err != nil {
		println(err.Error())
	} else {
		println("point your browser to http://localhost:8080/persons")
	}
}
