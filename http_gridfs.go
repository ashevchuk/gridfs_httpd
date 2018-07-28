package main

import (
    "flag"
    "fmt"
    "io"
    "os"
    "runtime"
    "strconv"
    "strings"
    "time"
    "net/http"
    "labix.org/v2/mgo"
    "labix.org/v2/mgo/bson"
)

var cpucores = flag.Int("cpucores", 1, "cpu cores to use")
var collection = flag.String("collection", "fs", "GridFS collection")
var database = flag.String("database", "", "GridFS database")
var host = flag.String("host", "127.0.0.1", "GridFS host")
var port = flag.String("port", "27017", "GridFS port")
var listen = flag.String("listen", ":8080", "listen adress:port")

type Config struct {
	Collection string
	Database   string
}

const FILE_BUFF_SIZE int = 1024 * 64 // 64Kbyte

var nongo_config *Config
var mongo_session *mgo.Session

func serve_gridfs(http_writer http.ResponseWriter, http_reader *http.Request) {
	if http_reader.Method != "GET" {
		http_writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	t_mongo_session := mongo_session.Copy()

	etag := http_reader.Header.Get("If-None-Match")

	if etag != "" {
		tag := strings.Split(etag, "_")

		if len(tag) == 2 {
			etag_id, etag_md5 := tag[0], tag[1]

			count, error := t_mongo_session.DB(nongo_config.Database).C(nongo_config.Collection + ".files").Find(bson.M{"_id": bson.ObjectIdHex(etag_id), "md5": etag_md5}).Count()

			if count > 0 && error == nil {
				http_writer.Header().Set("Cache-Control", "max-age=2629000: public") // 1 month
				http_writer.Header().Set("ETag", etag)
				http_writer.WriteHeader(http.StatusNotModified)
				return
			}
		}
	}

	split := strings.Split(http_reader.URL.Path, "/")

	file_request := split[len(split)-1:][0] // last part of path. eg: http://127.0.0.1:8080/some_path/path/filename_or_id

	gridfs := t_mongo_session.DB(nongo_config.Database).GridFS(nongo_config.Collection)

	gridfs_file, error := gridfs.OpenId(bson.ObjectIdHex(file_request))

	if error != nil || gridfs_file == nil {
		http_writer.WriteHeader(http.StatusNotFound)
		return
	}

	etag = gridfs_file.Id().(bson.ObjectId).Hex() + "_" + gridfs_file.MD5()

	http_writer.Header().Set("ETag", etag)
	http_writer.Header().Set("Cache-Control", "max-age=2629000: public") // 1 Month
	http_writer.Header().Set("Content-MD5", gridfs_file.MD5())

	gridfs_file_content_type := gridfs_file.ContentType()

	if gridfs_file_content_type != "" {
		http_writer.Header().Set("Content-Type", gridfs_file_content_type)
	}

	http_writer.Header().Set("Content-Length", strconv.FormatInt(gridfs_file.Size(), 10))

	var read_bytes int

	var read_buf = make([]byte, FILE_BUFF_SIZE)

	for {
		read_bytes, error = gridfs_file.Read(read_buf)

		if read_bytes == 0 && error != nil {
			break
		} else {
			http_writer.Write(read_buf[:read_bytes])
		}
	}

	if error != io.EOF {
		panic(error)
	}

	defer gridfs_file.Close()
	defer t_mongo_session.Close()
}

func main() {
	nongo_config = new(Config)

	flag.Parse()

	if *collection != "" {
		nongo_config.Collection = *collection
	} else {
		fmt.Println("Unknown collection")
		os.Exit(-1)
	}

	if *database != "" {
		nongo_config.Database = *database
	} else {
		fmt.Println("Unknown database")
		os.Exit(-1)
	}

	runtime.GOMAXPROCS(*cpucores)

	mongo_session, _ = mgo.Dial(fmt.Sprintf("mongodb://%s:%s?connect=direct", *host, *port))
	mongo_session.SetMode(mgo.Monotonic, true)

	gridfs_handler := http.NewServeMux()

	gridfs_handler.HandleFunc("/", serve_gridfs)

	http_server := &http.Server {
	    Addr:           *listen,
	    Handler:        gridfs_handler,
	    ReadTimeout:    60 * time.Second,
	    WriteTimeout:   10 * time.Second,
	    MaxHeaderBytes: 32 * 1024,
	}

	http_server.ListenAndServe()

	defer mongo_session.Close()
}
